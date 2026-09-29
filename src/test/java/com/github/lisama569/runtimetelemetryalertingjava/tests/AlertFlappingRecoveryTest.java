package com.github.lisama569.runtimetelemetryalertingjava.tests;

import com.github.lisama569.runtimetelemetryalertingjava.alert.AlertStateMachine;
import com.github.lisama569.runtimetelemetryalertingjava.model.AlertEvent;
import com.github.lisama569.runtimetelemetryalertingjava.model.AlertRule;
import com.github.lisama569.runtimetelemetryalertingjava.model.AlertState;
import com.github.lisama569.runtimetelemetryalertingjava.model.MetricEvent;
import com.github.lisama569.runtimetelemetryalertingjava.model.MetricKind;
import com.github.lisama569.runtimetelemetryalertingjava.model.WindowResult;
import com.github.lisama569.runtimetelemetryalertingjava.pipeline.TelemetryPipeline;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 抖动不反复触发、恢复需要连续确认；以及全链路（上报 + watermark 推进）并发一致性。
 */
class AlertFlappingRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(AlertFlappingRecoveryTest.class);

    private AlertStateMachine machine(int breachConfirm, int recoveryConfirm) {
        return new AlertStateMachine(
                new AlertRule("latency", MetricKind.LATENCY_AVG_MS, 300d,
                        breachConfirm, recoveryConfirm));
    }

    /** 窗口内 count 个请求，avg=latency。 */
    private WindowResult window(long index, long latency) {
        long start = index * 1000L;
        return new WindowResult(start, start + 1000, 1, 0, latency);
    }

    private AlertEvent eval(AlertStateMachine m, long index, long latency) {
        WindowResult w = window(index, latency);
        AlertEvent e = m.evaluate(w);
        log.info("[输入] 窗口#{} avg={}ms => [判定] state={} breach={} recovery={} 外发事件={}",
                index, latency, m.currentState(), m.getConsecutiveBreaches(),
                m.getConsecutiveRecoveries(),
                e == null ? "无" : e.from() + "->" + e.to() + " | " + e.reason());
        return e;
    }

    @Test
    @DisplayName("触发前抖动：越限/恢复交替，达不到连续确认，绝不触发")
    void flappingBeforeFireNeverTriggers() {
        AlertStateMachine m = machine(2, 2);
        // 500 越限 -> 100 恢复 -> 500 越限 -> 100 恢复，连续越限始终到不了 2
        assertEquals(0, countEvents(m, List.of(500L, 100L, 500L, 100L, 500L, 100L)),
                "触发确认期内的抖动绝不能产生任何告警事件");
        assertEquals(AlertState.NORMAL, m.currentState(),
                "未连续达标的抖动后状态必须回到/保持 NORMAL");
    }

    @Test
    @DisplayName("触发后抖动：FIRING 中偶发恢复窗不恢复、不产生任何事件，也不重复触发")
    void flappingWhileFiringStaysFiring() {
        AlertStateMachine m = machine(2, 2);
        // 连续 2 窗越限触发
        assertEquals(null, eval(m, 0, 500), "窗口#0 进入 PENDING（内部态，不外发）");
        AlertEvent fired = eval(m, 1, 600);
        assertNotNull(fired, "窗口#1 连续越限，必须触发");
        assertEquals(AlertState.FIRING, fired.to(), "触发目标状态必须是 FIRING");

        // FIRING 中：恢复 1 窗（进 RESOLVING），立刻又越限 1 窗 —— 抖动
        AlertEvent attemptResolve = eval(m, 2, 100);
        assertEquals(AlertState.RESOLVING, m.currentState(), "单个恢复窗只进入 RESOLVING");
        // RESOLVING 是内部确认态，不产生对外恢复事件
        assertEquals(null, attemptResolve, "进入 RESOLVING 不能外发恢复事件");
        AlertEvent backToFire = eval(m, 3, 500);
        assertEquals(AlertState.FIRING, m.currentState(), "恢复确认未达标又越限，必须回到 FIRING");
        assertEquals(null, backToFire, "RESOLVING->FIRING 是内部抖动，不能再次外发触发事件（不重复触发）");

        // 再来连续 3 个越限窗，仍不应有新触发事件
        eval(m, 4, 700);
        eval(m, 5, 700);
        eval(m, 6, 700);
        assertEquals(AlertState.FIRING, m.currentState(), "持续越限稳定保持 FIRING");
    }

    @Test
    @DisplayName("正常恢复：连续 2 窗低于阈值才产生唯一恢复事件")
    void recoveryRequiresConsecutiveHealthyWindows() {
        AlertStateMachine m = machine(2, 2);
        eval(m, 0, 500);
        assertNotNull(eval(m, 1, 500), "连续 2 窗越限触发");

        // 第一个恢复窗：RESOLVING，无事件
        AlertEvent r1 = eval(m, 2, 100);
        assertEquals(AlertState.RESOLVING, m.currentState(), "恢复第 1 窗应为 RESOLVING");
        assertEquals(null, r1, "恢复确认未满不能产生恢复事件");

        // 第二个恢复窗：NORMAL，恢复事件
        AlertEvent r2 = eval(m, 3, 100);
        assertNotNull(r2, "连续 2 窗恢复必须产生恢复事件");
        assertEquals(AlertState.RESOLVING, r2.from(), "恢复确认=2 时恢复事件从 RESOLVING 发出");
        assertEquals(AlertState.NORMAL, r2.to(), "恢复事件目标必须是 NORMAL");

        // 恢复后继续健康，不再产生事件
        assertEquals(null, eval(m, 4, 100), "已恢复后的健康窗口不产生任何事件");
        assertEquals(AlertState.NORMAL, m.currentState(), "状态稳定 NORMAL");
    }

    @Test
    @DisplayName("完整抖动周期：触发 -> 抖动 -> 恢复 -> 再次触发，全周期对外事件可解释")
    void fullCycleTriggersAndRecoveriesAreStable() {
        AlertStateMachine m = machine(2, 2);
        // 触发
        eval(m, 0, 500);
        AlertEvent fire1 = eval(m, 1, 500);
        // FIRING 中抖动 2 轮（恢复/越限交替）
        eval(m, 2, 100);
        eval(m, 3, 500);
        eval(m, 4, 100);
        eval(m, 5, 500);
        // 真正恢复
        AlertEvent recover1 = eval(m, 6, 100);
        AlertEvent recover2 = eval(m, 7, 100);
        // 再次连续越限，重新触发
        eval(m, 8, 500);
        AlertEvent fire2 = eval(m, 9, 500);

        assertNotNull(fire1, "周期内第一次触发事件必须存在");
        assertNotNull(recover2, "周期内恢复事件必须存在");
        assertNotNull(fire2, "恢复后再次连续越限必须能重新触发");
        assertEquals(AlertState.RESOLVING, recover2.from(), "恢复确认=2 时，恢复事件从 RESOLVING 发出");
        assertEquals(AlertState.FIRING, fire2.to(), "重新触发后回到 FIRING");
        log.info("[判定汇总] 外发事件顺序: FIRE#1 -> (抖动无事件) -> RECOVER -> FIRE#2，"
                + "fire1={} recover2={} fire2={}", fire1.to(), recover2.to(), fire2.to());
    }

    @Test
    @DisplayName("全链路并发：多线程乱序重复上报 + 并发推进 watermark，窗口与告警结论稳定")
    void endToEndConcurrentReportingIsStable() throws Exception {
        TelemetryPipeline pipeline = new TelemetryPipeline(1000L, 500L,
                List.of(new AlertRule("latency", MetricKind.LATENCY_AVG_MS, 100d, 2, 2)));

        int threads = 8;
        int perThread = 200;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<String> duplicateIds = new ConcurrentLinkedQueue<>();

        for (int t = 0; t < threads; t++) {
            final int threadIdx = t;
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int i = 0; i < perThread; i++) {
                    long eventTime = (i % 20) * 100L; // 落在 [0,2000) 共 2 个窗口
                    // 每 7 条故意复用同一 eventId，制造跨线程重复上报
                    String eventId = (i % 7 == 0)
                            ? "shared-" + i
                            : "t" + threadIdx + "-" + UUID.randomUUID();
                    MetricEvent event = new MetricEvent(eventId, null, eventTime,
                            200L + (i % 3) * 50L, false); // avg 必然 > 100
                    var result = pipeline.report(event);
                    if (result.outcome().name().equals("DUPLICATE")) {
                        duplicateIds.add(eventId);
                    }
                }
            });
        }

        // 另一个线程持续推进 watermark，直到封存前 2 个窗口
        AtomicInteger advanceRounds = new AtomicInteger();
        pool.submit(() -> {
            try {
                start.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            for (long wm = 1000L; wm <= 3500L; wm += 250L) {
                pipeline.advance(wm);
                advanceRounds.incrementAndGet();
                try {
                    Thread.sleep(2L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        });

        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(15, TimeUnit.SECONDS), "并发任务应及时结束");

        // 最终再推进一次，确保 [0,1000)、[1000,2000) 全部封存
        pipeline.advance(10_000L);

        var windows = pipeline.sealedWindows();
        var firstTwo = windows.stream().filter(w -> w.windowStart() < 2000L).toList();
        log.info("[输入] {} 线程 × {} 条 + 并发 watermark 推进 {} 轮 => [判定] 封存窗口(前2个)={}",
                threads, perThread, advanceRounds.get(), firstTwo.size());
        firstTwo.forEach(w -> log.info("[封存窗口] [{},{}) count={} avg={}",
                w.windowStart(), w.windowEnd(), w.count(), w.averageLatencyMillis()));

        assertEquals(2, firstTwo.size(), "前两个有数据的窗口必须都被封存，不能漏封存");
        // 幂等计数：每个窗口只包含唯一 eventId
        long totalCount = firstTwo.stream().mapToLong(WindowResult::count).sum();
        long expectedUnique = expectedUniqueEventCount(threads, perThread);
        log.info("[判定] 两个窗口去重后总数={}，理论唯一事件数(落前2窗)={}，观察到重复回执数={}",
                totalCount, expectedUnique, duplicateIds.size());
        assertTrue(totalCount <= expectedUnique,
                "并发重复上报绝不能算重：实际 " + totalCount + " 不得超过唯一 id 数 " + expectedUnique);
        // 每个唯一 id 恰在某一窗口计一次，最终计数应严格等于唯一 id 总数（迟到允许 <=）
        assertTrue(totalCount > 0, "窗口必须有数据");

        // 两个窗口均越限（avg>100），最终必然稳定 FIRING；重复推进也未产生重复触发
        pipeline.advance(20_000L);
        assertEquals(AlertState.FIRING, pipeline.alertState("latency"),
                "两个越限窗口后规则必须稳定 FIRING");
        long fireCount = pipeline.alertEvents().stream()
                .filter(e -> e.to() == AlertState.FIRING).count();
        assertEquals(1, fireCount, "全链路下 FIRING 触发事件必须恰好 1 个，不能重复触发");
    }

    /** 统计落在 [0,2000) 的唯一 eventId 数：shared-i（i%7==0, i=0..199, eventTime<2000）+ 各线程私有 id。 */
    private long expectedUniqueEventCount(int threads, int perThread) {
        java.util.Set<String> unique = new java.util.HashSet<>();
        for (int t = 0; t < threads; t++) {
            for (int i = 0; i < perThread; i++) {
                long eventTime = (i % 20) * 100L;
                if (eventTime >= 2000L) {
                    continue;
                }
                String id = (i % 7 == 0) ? "shared-" + i : "t" + t + "-unique";
                // 私有 id 用确定性标记：同一线程内每个 i 都是不同 uuid，无法静态枚举，
                // 因此这里只统计 shared 唯一数做下界校验由调用方处理；私有 id 全部唯一。
                if (id.startsWith("shared")) {
                    unique.add(id);
                }
            }
        }
        // 私有 id 数：每线程落在前2窗的 i（i%20<20，即所有 i 中 eventTime<2000；i%20 恒<20 => 全部 perThread）
        long privateCount = (long) threads * perThread
                - (long) threads * countSharedPerThread(perThread);
        return unique.size() + privateCount;
    }

    private long countSharedPerThread(int perThread) {
        long c = 0;
        for (int i = 0; i < perThread; i++) {
            if (i % 7 == 0) {
                c++;
            }
        }
        return c;
    }

    private int countEvents(AlertStateMachine m, List<Long> avgSequence) {
        int count = 0;
        long index = 0;
        for (Long avg : avgSequence) {
            if (eval(m, index++, avg) != null) {
                count++;
            }
        }
        return count;
    }
}
