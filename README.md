# 运行时可观测：数据采集与告警判定链路（本地内存实现）

一个零外部依赖的 Spring Boot 工程，用本地内存模拟「指标采集 → 时间窗口聚合 → 告警状态机判定」
全链路，并贯穿链路上下文传播。可通过单元测试和 HTTP 接口在本地完整验证。

## 1. 模块总览

```
model/       指标事件、窗口快照、告警规则/状态/事件等领域模型
windowing/   WindowAggregator：滚动窗口聚合 + watermark + 幂等去重 + 迟到处理
trace/       TraceContextHolder / TraceIds / TraceRunnable|Callable /
             TracePropagatingExecutor / InboundTraceFilter
alert/       AlertStateMachine（四态状态机）/ AlertEngine（多规则）
pipeline/    TelemetryPipeline（门面，串联聚合与告警）/ PipelineConfig（默认 Bean）
web/         TelemetryController（本地演示 HTTP 接口）
```

## 2. 指标采集与窗口聚合模型

实现：`windowing.WindowAggregator`，全部方法 `synchronized`，并发上报安全。

### 2.1 滚动窗口（按事件时间，不按到达时间）

- 窗口大小 `windowSizeMillis`（默认 1000ms）。
- 一条上报按它携带的 `eventTimeMillis` 归窗：

  ```
  windowStart = floor(eventTimeMillis / windowSizeMillis) * windowSizeMillis
  窗口区间    = [windowStart, windowStart + windowSizeMillis)
  ```

  因此**乱序到达不影响归属**：t=1900 先到、t=100 后到，仍各自进入正确窗口。

- 窗口内累加：`count`（请求数）、`errorCount`（错误数）、`totalLatencyMillis`（耗时总和），
  封存快照上派生 `averageLatencyMillis = total/count` 与 `errorRate = errorCount/count`。

### 2.2 幂等去重（重复上报不算重）

- 每条上报必须带 `eventId`；聚合器维护全局 `eventId -> windowStart` 表。
- 同一 `eventId` 无论重复上报几次、字段是否不同，**只在第一次计一次**，
  后续全部返回 `DUPLICATE`，并指出首次计入的窗口。
- 窗口封存后幂等表仍保留：迟到的重复上报依旧识别为 `DUPLICATE`，不会被误判成迟到或二次计数。

### 2.3 watermark 与迟到数据（封存结果可解释、不回改）

- watermark 随 `advanceWatermark(t)` **单调推进**（倒退调用不封存任何窗口）。
- 封存条件：

  ```
  windowEnd <= watermark - allowedLatenessMillis
  ```

  即在「容忍迟到宽限期」`allowedLatenessMillis`（默认 500ms）内到达的迟到事件
  仍可补入尚未封存的窗口；超过宽限期、窗口一旦封存：
  - 窗口发布为不可变 `WindowResult`；
  - 再落到该窗口的迟到事件判 `TOO_LATE` **直接丢弃，结果绝不回改**。
- 封存按时间顺序返回；只有真正收到过数据的窗口才会产生快照（空窗口不产生快照，
  也不参与告警判定，避免“无样本”被误当成恢复）。

> 每条上报的 `AcceptResult` 都带 `outcome`（`ACCEPTED/DUPLICATE/TOO_LATE`）、
> 归属窗口起点与中文 `reason`，窗口快照和告警事件也都带判定依据，做到“结果可解释”。

## 3. 链路上下文传播

实现：`trace` 包。

### 3.1 标识格式（W3C trace-context 形态）

- `traceId`：32 位小写十六进制、非全 0；`spanId`：16 位小写十六进制、非全 0。
- `TraceIds.resolve(incoming)`：
  - 入站合法 → 原样透传 traceId，并生成新的 spanId；
  - 缺失 / 空白 / 非法（含大写、长度不对、全 0、含非法字符）→
    用 `SecureRandom` 自行生成合法 traceId，返回结果中带 `generated=true` 与原因。

### 3.2 线程传播规则

- 入站：`InboundTraceFilter`（`OncePerRequestFilter`）解析 `X-Trace-Id`，
  放入 `TraceContextHolder`，把最终 traceId **写回响应头**，请求结束在 `finally` 里清理。
- 同线程：`TraceContextHolder`（`InheritableThreadLocal`）+ `runWith` 执行后恢复原上下文。
- **新建子线程**：`InheritableThreadLocal` 在子线程“出生”时继承父线程值。
- **线程池（复用线程）**：不能依赖继承（池线程创建时机不定，会断掉或串线），必须显式传播：
  - `TraceRunnable.wrap` / `TraceCallable.wrap`：在**提交任务那一刻**捕获当前上下文，
    在**工作线程执行时**恢复，执行结束恢复该工作线程原上下文（不污染复用线程）；
  - 或用 `TracePropagatingExecutor` 包装任意 `Executor`，提交时自动包装。
- 上报指标若未带 traceId，`TelemetryPipeline.report` 会用当前 holder 链路兜底，
  holder 也为空时生成合法 traceId。

## 4. 告警状态机：触发与恢复条件

实现：`alert.AlertStateMachine`（每条规则一个实例，方法全部 `synchronized`）。

四个状态：`NORMAL`、`PENDING`、`FIRING`、`RESOLVING`。

```
NORMAL  ──越限窗──▶ PENDING ──连续越限达标(breachConfirmations)──▶ FIRING（产生“触发”事件）
  ▲                    │ 恢复窗                                      │
  │                    ▼                                            │ 越限窗（保持FIRING，
  │                  NORMAL（越限未连续达标即回落，抖动不触发）        │ 不重复触发）
  │                                                                 │
  └──────────────── NORMAL ◀──连续恢复达标(recoveryConfirmations)───┘
                            FIRING ─恢复窗─▶ RESOLVING ─恢复窗─▶ NORMAL（产生“恢复”事件）
                            RESOLVING ─又越限─▶ FIRING（抖动回退，不外发，不重复触发）
```

要点：

- **必须持续越限**：连续 `breachConfirmations` 个封存窗口越限才真正进入 `FIRING`
  并外发一次“触发”事件；单窗越限只进 `PENDING`。
- **抖动不反复触发/恢复**：确认期内任一窗口不满足就清零计数并回退；
  `RESOLVING→FIRING` 属于“恢复确认失败”，告警本就没恢复，**不会再外发一次触发事件**。
- **恢复也需确认**：连续 `recoveryConfirmations` 个窗口低于阈值才回 `NORMAL`，
  外发一次“恢复”事件。
- **对外事件只有两类**：进入 `FIRING`（从 PENDING/NORMAL）与进入 `NORMAL`（恢复）；
  PENDING / RESOLVING 之间的内部变化不外发。
- **并发一致（不重不漏）**：
  - 状态机用 synchronized 串行化每次评估；
  - 每个封存窗口按 `windowEnd` **幂等评估一次**，同一窗口重复评估返回 null；
  - `TelemetryPipeline.advance` 用对象锁串行化“封存 + 全规则判定”，
    保证每个封存窗口恰好被每条规则评估一次。
- 默认规则（`PipelineConfig`）：平均耗时 > 300ms（连续 2 窗触发 / 2 窗恢复），
  错误率 > 0.5（连续 2 窗触发 / 2 窗恢复）。阈值判定为严格大于（`>`），空窗口不越限。

## 5. 本地怎么验证

### 5.1 运行测试（推荐）

```bash
mvn test
```

27 个用例，分组覆盖：

| 测试类 | 覆盖点 |
| --- | --- |
| `WindowAggregationTest` | 乱序归窗、宽限期内迟到计入、封存后迟到丢弃、eventId 幂等（含封存后重复）、滚动封存有序、空窗口不产生快照 |
| `TracePropagationTest` | 合法透传、缺失/非法生成、新建线程继承、线程池复用不串线、执行器自动传播、裸任务对照、上报链路兜底 |
| `AlertStateMachineTest` | 连续确认才触发、单窗口幂等、8 窗口×16 线程并发不重不漏、空窗口不误恢复、多规则独立 |
| `AlertFlappingRecoveryTest` | 触发前抖动不触发、FIRING 中抖动不重复触发、恢复需连续确认、完整触发-抖动-恢复-再触发周期、全链路并发（8 线程×200 条 + 并发推进 watermark） |
| `TelemetryHttpIntegrationTest` | HTTP 端到端：trace 头解析/回显、乱序重复上报、封存、告警查询 |

测试日志会打印**输入**（事件时间/耗时/错误、入站头、并发规模）和**判定依据**
（归属窗口、outcome 与 reason、状态与连续计数、状态跳变原因）。
断言消息均为带场景的中文说明，失败时可直接区分是“算错/算重/误丢”还是“误触发/漏触发/串线”。

### 5.2 启动后用 HTTP 手工演示

```bash
mvn spring-boot:run        # server.port=0，启动日志里看实际端口
```

示例（端口按实际替换）：

```bash
# 1) 上报 3 条慢请求到窗口 [0,1000)（毫秒时间戳，可任意乱序）
for t in 100 500 900; do
  curl -s -X POST localhost:$PORT/telemetry/events -H 'Content-Type: application/json' \
    -d "{\"eventId\":\"e-$t\",\"eventTime\":$t,\"latencyMillis\":500,\"error\":false}"; echo
done

# 2) 重复上报同一 eventId -> outcome=DUPLICATE
curl -s -X POST localhost:$PORT/telemetry/events -H 'Content-Type: application/json' \
  -d '{"eventId":"e-100","eventTime":100,"latencyMillis":1,"error":true}'; echo

# 3) 推进 watermark=1500（horizon=1000）封存 [0,1000)
curl -s -X POST localhost:$PORT/telemetry/advance -H 'Content-Type: application/json' \
  -d '{"watermark":1500}'; echo

# 4) 查询窗口与告警
curl -s localhost:$PORT/telemetry/windows; echo
curl -s localhost:$PORT/telemetry/alerts; echo

# 5) 链路：带与不带 X-Trace-Id
curl -s localhost:$PORT/telemetry/trace -H 'X-Trace-Id: 4bf92f3577b34da6a3ce929d0e0e4736'; echo
curl -s -D - localhost:$PORT/telemetry/trace -o /dev/null | grep -i x-trace-id
```

接口一览：

| 方法 路径 | 作用 |
| --- | --- |
| `POST /telemetry/events` | 上报一条指标；body：`eventId,eventTime,latencyMillis,error,traceId?` |
| `POST /telemetry/advance` | 推进 watermark，封存到期窗口并跑告警判定，返回本次告警事件 |
| `GET  /telemetry/windows` | 已封存窗口快照列表 |
| `GET  /telemetry/alerts`  | 全部告警事件 + 两条规则当前状态 |
| `GET  /telemetry/trace`   | 回显当前请求解析出的 traceId/spanId（响应头也带 X-Trace-Id） |

## 6. 设计取舍说明

- 全部状态在 JVM 内存，重启即清空，仅用于运行时语义演示与测试，不依赖任何中间件。
- 时间由上报与 `advance` 显式传入（事件时间 + watermark），因此测试可确定性地
  复现乱序、迟到与并发，不依赖墙上时钟和 sleep 来“碰运气”。
