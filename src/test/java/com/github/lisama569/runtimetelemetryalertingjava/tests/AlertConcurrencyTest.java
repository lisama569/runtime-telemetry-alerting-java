package com.github.lisama569.runtimetelemetryalertingjava.tests;

import com.github.lisama569.runtimetelemetryalertingjava.alert.AlertStateMachine;
import com.github.lisama569.runtimetelemetryalertingjava.model.AlertEvent;
import com.github.lisama569.runtimetelemetryalertingjava.model.AlertRule;
import com.github.lisama569.runtimetelemetryalertingjava.model.AlertState;
import com.github.lisama569.runtimetelemetryalertingjava.model.ComparisonOperator;
import com.github.lisama569.runtimetelemetryalertingjava.model.MetricPoint;
import com.github.lisama569.runtimetelemetryalertingjava.model.WindowAggregation;
import com.github.lisama569.runtimetelemetryalertingjava.model.WindowEmit;
import com.github.lisama569.runtimetelemetryalertingjava.model.WindowResultType;
import com.github.lisama569.runtimetelemetryalertingjava.pipeline.MetricsAlertPipeline;
import com.github.lisama569.runtimetelemetryalertingjava.window.WindowAggregator;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发上报下的一致性测试：
 * - 状态机：同一批窗口被多线程重复投递时，每个窗口只生效一次，最终恰好触发一次；
 *   不同窗口并发推进时，确认计数不漏加。
 * - 流水线：多线程并发上报同一窗口的点，去重与计数必须正确，不丢不重。
 */
class AlertConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(AlertConcurrencyTest.class);
    private static final long W = 1000;

    private WindowEmit onTimeWindow(long start, double value) {
        WindowAggregation agg = new WindowAggregation(start, start + W, "latency");
        agg.add(new MetricPoint("latency", start + 1, value, "w-" + start));
        return new WindowEmit(WindowResultType.ON_TIME, agg, start + W, "concurrency-on-time");
    }

    @Test
    void concurrentWindowsProduceExactlyOneTrigger() throws Exception {
        AlertRule rule = new AlertRule("high-latency", "latency",
                ComparisonOperator.GT, 100.0, 3, 2);
        AlertStateMachine machine = new AlertStateMachine(rule);
        Set<AlertEvent> events = ConcurrentHashMap.newKeySet();
        machine.addListener(events::add);

        int threads = 12;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        ExecutorService pool = Executors.newFixedThreadPool(threads);

        for (int t = 0; t < threads; t++) {
            final int idx = t;
            pool.submit(() -> {
                try {
                    start.await();
                    // 每个线程都重复投递“同一批”三个不同窗口的定稿结果，
                    // 模拟监听器重复注册 / 多线程重放的极端情况
                    long[] windows = {0, 1000, 2000};
                    for (int round = 0; round < 50; round++) {
                        // 打乱起始顺序也不能漏判（三个窗口本身都是越限的）
                        long s = windows[(round + idx) % windows.length];
                        machine.evaluate(onTimeWindow(s, 150));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS), "判定依据: 并发任务必须全部完成");
        pool.shutdown();

        long fired = events.stream().filter(AlertEvent::triggered).count();
        long recovered = events.stream().filter(AlertEvent::recovered).count();
        log.info("[concurrency-case1] states={} fired={} recovered={} totalEvents={}",
                machine.state(), fired, recovered, events.size());
        assertEquals(1, fired,
                "判定依据: 无论多少线程重复投递这三个窗口，真正触发必须恰好 1 次，不能重复触发");
        assertEquals(0, recovered, "判定依据: 三个窗口全部越限，不应出现恢复");
        assertEquals(AlertState.FIRING, machine.state(), "判定依据: 最终状态必须稳定停在 FIRING");
        assertEquals(3, machine.evaluatedWindowCount(),
                "判定依据: 三个不同窗口必须各被精确评估一次，重复投递不得漏窗或重算");
    }

    @Test
    void concurrentReportsDoNotLosePoints() throws Exception {
        // 宽限给 0，便于用 watermark 立刻定稿；但并发阶段只做 ingest，定稿统一在最后推进
        WindowAggregator aggregator = new WindowAggregator(W, 0);
        MetricsAlertPipeline pipeline = new MetricsAlertPipeline(aggregator);
        AlertRule rule = new AlertRule("high-latency", "latency",
                ComparisonOperator.GT, 100.0, 2, 2);
        AlertStateMachine machine = pipeline.registerRule(rule);
        Set<AlertEvent> alertEvents = ConcurrentHashMap.newKeySet();
        machine.addListener(alertEvents::add);

        int writers = 16;
        int perWriter = 200;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(writers);
        ExecutorService pool = Executors.newFixedThreadPool(writers);
        AtomicInteger duplicatesObserved = new AtomicInteger();

        // 所有点都落在窗口 [0,1000) 内；其中一半 eventId 会在两个线程间撞号（模拟重复投递）
        for (int t = 0; t < writers; t++) {
            final int idx = t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perWriter; i++) {
                        // eventId 0..perWriter/2-1 在相邻线程对间重复
                        String eventId = "evt-" + (i / 2);
                        long ts = (i * 7L + idx) % W; // 乱序，但都在同一窗口
                        List<WindowEmit> emits = pipeline.report(new MetricPoint("latency", ts, 120, eventId));
                        emits.stream().filter(e -> e.type() == WindowResultType.DUPLICATE)
                                .findAny().ifPresent(x -> duplicatesObserved.incrementAndGet());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS), "判定依据: 并发上报必须全部完成");
        pool.shutdown();

        log.info("[concurrency-case2] before close: activeWindows={}, duplicatesObserved={}",
                aggregator.activeWindows().size(), duplicatesObserved.get());
        List<WindowEmit> closed = pipeline.advanceWatermark(W); // end+0=1000 定稿
        WindowAggregation result = closed.stream()
                .filter(e -> e.type() == WindowResultType.ON_TIME).findFirst()
                .orElseThrow(() -> new AssertionError("判定依据: 窗口必须定稿")).aggregation();

        long expectedUnique = perWriter / 2; // evt-0 .. evt-99
        long expectedReports = (long) writers * perWriter;
        long expectedDuplicates = expectedReports - expectedUnique;
        log.info("[concurrency-case2] finalized count={} expected={} duplicatesObserved={} expectedDup={} fired={} state={}",
                result.count(), expectedUnique, duplicatesObserved.get(), expectedDuplicates,
                alertEvents.stream().filter(AlertEvent::triggered).count(), machine.state());

        assertEquals(expectedUnique, result.count(),
                "判定依据: 3200 次并发上报中只有 100 个不同 eventId，计数必须精确等于 100，不丢不重");
        assertEquals(expectedDuplicates, duplicatesObserved.get(),
                "判定依据: 其余 3100 次上报必须全部被识别为 DUPLICATE");
        assertEquals(120.0, result.mean(), 1e-9, "判定依据: 全部采样值相同，均值不受并发影响");
    }
}
