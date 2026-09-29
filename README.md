# runtime-telemetry-alerting-java

一套纯内存模拟的运行时可观测数据采集与告警判定链路（Spring Boot 3 / Java 21，无任何外部服务依赖）：

```
入站请求(X-Trace-Id) ──► TraceHeaderFilter 解析/生成 traceId
                              │ ThreadLocal
                              ▼
原始指标上报 MetricPoint ──► WindowAggregator（事件时间滚动窗口 + watermark + 去重）
                              │ WindowEmit（ON_TIME / LATE_CORRECTION / DUPLICATE / DROPPED）
                              ▼
                    MetricsAlertPipeline 按指标分发
                              │ 只消费 ON_TIME
                              ▼
                    AlertStateMachine（OK/PENDING/FIRING/RECOVERING）
                              │ AlertEvent（FIRED / RECOVERED / STATE_CHANGE）
                              ▼
                        AlertListener（内存事件日志）
```

异步线程/线程池通过 `TracingExecutor` 快照并恢复链路上下文。

---

## 1. 指标采集与窗口聚合模型

实现见 `window/WindowAggregator.java`、`model/WindowAggregation.java`。

### 1.1 数据模型

- `MetricPoint(metricName, eventTimeMillis, value, eventId)`：一次原始上报。
  - 指标：`latency`（请求耗时）、`error-rate`（错误率；上报 0/1，窗口均值即错误率）。
  - **eventId 必填，用于全局去重**：重试 / 重复投递不会被二次计数。
- `WindowAggregation`：窗口内累加 `count / sum / min / max`，均值 `mean = sum / count`。

### 1.2 固定滚动窗口（Tumbling Window）

- 窗口大小 `windowSizeMillis`，按边界对齐：`start = floor(eventTime / W) * W`，窗口为半开区间 `[start, start+W)`。
- 聚合基于**事件时间**（上报自带 `eventTimeMillis`），而不是处理时间，因此乱序到达不影响归属窗口。

### 1.3 Watermark 与宽限期

- `watermark` = 目前观察到的最大事件时间，**单调不减**（可由数据驱动，也可用 `advanceWatermark` 手动推进）。
- 允许迟到 `allowedLatenessMillis`：窗口在 `watermark >= windowEnd + allowedLateness` 时**定稿**。

### 1.4 四类产出（每条结果都带 detail 文案，可解释来历）

| 类型 | 触发条件 | 是否计数 | 是否驱动告警 |
|---|---|---|---|
| `ON_TIME` | watermark 越过 `windowEnd + 宽限期`，窗口定稿 | 窗口的最终结果 | 是 |
| `LATE_CORRECTION` | 事件所属窗口的 `windowEnd` 已被 watermark 越过、但仍在宽限期内 | 补计入该窗口 | 否（定稿时一并体现） |
| `DUPLICATE` | eventId 已出现过（含定稿后再来） | 不计数 | 否 |
| `DROPPED` | 所属窗口已定稿且超过宽限期 | 不计数、不复活窗口 | 否 |

注意区分“乱序”和“迟到”：同一个**尚未到点**的窗口内先后来到的点，只是乱序，静默累加，不标迟到；
只有 watermark 已经越过该窗口结束时间后到的点，才算迟到。

### 1.5 滚动结果为什么是对的

- 定稿前：乱序点直接进窗口；迟到但在宽限期内的点补进窗口（计数 +1，sum/min/max 更新）。
- 定稿时：该窗口所有已接受的点（含迟到修正）一次性聚合输出，并从活动窗口移除。
- 定稿后：再来的数据要么 `DUPLICATE`（eventId 见过），要么 `DROPPED`（窗口过期），定稿结果永不被改写。
- 所有状态变更在同一把锁内完成，监听器通知在锁外发出，并发上报不丢不重。

---

## 2. 链路上下文传播规则

实现见 `trace/` 包。

### 2.1 标识格式

- traceId：16~32 位十六进制，不能全 0（合法则透传，大写规范化为小写）。
- spanId：16 位十六进制，不能全 0。
- 入站头：`X-Trace-Id`。缺失 / 非法 / 全 0 时，服务端用 `SecureRandom` 生成合法 traceId，
  并在响应头打标 `X-Trace-Id-Generated: true` 方便区分。

### 2.2 上下文与传递

- `TraceContext(traceId, spanId, parentSpanId, inbound)`；每跨一跳用 `newChildSpan()` 生成新 spanId，
  parent 指向上一跳 span，**traceId 全程不变**。
- `TraceContextHolder`：ThreadLocal 持有；`require()` 在无线程上下文时自动起链，保证不断。
- `TraceHeaderFilter`（OncePerRequestFilter）：请求入口解析/生成 → 放入 ThreadLocal → 响应回写 → 请求结束 `clear()`。

### 2.3 跨线程（线程池）

`TracingExecutor implements ExecutorService`：

1. **提交时**在调用方线程快照当前 TraceContext；
2. **执行前**在工作线程恢复快照并开一个子 span（traceId 不变，parent 指向提交点）；
3. **执行后**（finally）清理工作线程 ThreadLocal。

因此：有上下文的任务不会断链；没带上下文的任务也不会读到线程复用时的残留 traceId（不串链）。

---

## 3. 告警状态机：触发与恢复条件

实现见 `alert/AlertStateMachine.java`、`model/AlertRule.java`。

规则 `AlertRule(metric, operator, threshold, triggerConfirmations, recoverConfirmations)`。
每来一个该指标的 `ON_TIME` 窗口，取窗口均值做阈值比较。

### 3.1 四态

- `OK`：正常。出现越限窗口 → `PENDING`，`breachStreak=1`。
- `PENDING`：触发确认中。
  - 再来越限窗口：`breachStreak++`，达到 `triggerConfirmations` → **FIRED**（进入 `FIRING`）。
  - 中途出现一个正常窗口：计数清零，退回 `OK`——**抖动被吞掉，不触发**。
- `FIRING`：已触发。出现正常窗口 →
  - `recoverConfirmations == 1`：立即 **RECOVERED** 回到 `OK`；
  - 否则进入 `RECOVERING`，`okStreak=1`。
  - 期间继续越限：保持 `FIRING`，**不重复触发**。
- `RECOVERING`：恢复确认中。
  - 连续正常窗口：`okStreak++`，达到 `recoverConfirmations` → **RECOVERED** 回到 `OK`。
  - 中途再次越限：恢复被否决，立即回到 `FIRING`（标记为 `STATE_CHANGE`，**不算新的触发**）。

### 3.2 事件种类与可解释性

`AlertEvent` 带 `kind`：

- `FIRED`：真正的触发（只在 PENDING 攒满确认时产生一次）。
- `RECOVERED`：真正的恢复。
- `STATE_CHANGE`：中间态迁移（OK↔PENDING、FIRING↔RECOVERING），用于审计追溯。

每个事件都记录 `observedValue / windowStart / windowEnd / reason`，单测与运行日志都会打印输入与判定依据。

### 3.3 并发一致性（不重、不漏）

- 状态转换全部在 `synchronized` 块内完成；监听器列表、事件日志为并发安全集合。
- **同一 windowStart 只评估一次**（`seenWindowStarts` 去重）：多线程重放同一窗口结果，
  既不会把确认计数刷大，也不会重复触发；不同窗口并发推进时每个窗口都精确生效，不漏判。
- 只有 `ON_TIME` 驱动状态机；`LATE_CORRECTION` 不重复驱动，避免迟到修正导致告警抖动。

---

## 4. 本地怎么验证

### 4.1 跑全部单测

```bash
mvn test
```

| 测试类 | 覆盖场景 |
|---|---|
| `WindowAggregationTest` | 乱序、迟到修正、超宽限丢弃、eventId 去重（含定稿后重复） |
| `TracePropagationTest` | 非法/缺失头生成、线程池传播、交错任务不串链、过滤器行为 |
| `AlertStateMachineTest` | 连续确认才触发、抖动抑制、恢复确认/恢复被否决、同窗口幂等 |
| `AlertConcurrencyTest` | 12 线程重放窗口恰好触发 1 次；16 线程 3200 次上报去重计数精确 |
| `PipelineEndToEndTest` | latency + error-rate 双规则：抖动不触发、触发、恢复、重放不改结论 |

失败原因区分：每条断言都带“判定依据: ……”说明；日志中搜索
`[window]`、`[trace]`、`[alert:...]`、`[pipeline]` 可看到每个输入和判定依据。

### 4.2 启动应用手工验证

```bash
mvn spring-boot:run
```

链路透传演示（对比同步与异步线程中的 traceId/spanId）：

```bash
curl -i http://localhost:8080/demo/sync
curl -i -H 'X-Trace-Id: 0123456789abcdef0123456789abcdef' http://localhost:8080/demo/async
# 非法头 -> 服务端生成，响应头 X-Trace-Id-Generated: true
curl -i -H 'X-Trace-Id: not-valid' http://localhost:8080/demo/sync
```

采集 / 窗口 / 告警演示（演示配置：1s 窗口、0 宽限；latency>100 连续 3 窗口触发、连续 2 窗口恢复）：

```bash
# 连续 3 个窗口上报越限耗时（eventTime 落在 0~999 / 1000~1999 / 2000~2999）
for w in 0 1000 2000; do
  curl -s -X POST "http://localhost:8080/api/metrics/report?metric=latency&value=150&eventTime=$((w+100))&eventId=lat-$w"
  curl -s -X POST "http://localhost:8080/api/metrics/advance?watermark=$((w+1000))" >/dev/null
done
curl -s http://localhost:8080/api/alerts/status | head

# 重复上报同一个 eventId -> DUPLICATE，不改变计数与告警
curl -s -X POST "http://localhost:8080/api/metrics/report?metric=latency&value=150&eventTime=100&eventId=lat-0"

# 连续 2 个正常窗口 -> RECOVERED
for w in 3000 4000; do
  curl -s -X POST "http://localhost:8080/api/metrics/report?metric=latency&value=20&eventTime=$((w+100))&eventId=ok-$w" >/dev/null
  curl -s -X POST "http://localhost:8080/api/metrics/advance?watermark=$((w+1000))" >/dev/null
done
curl -s http://localhost:8080/api/alerts/status
```

所有状态都在内存中，重启即清空；不依赖数据库、消息队列或任何外部服务。
