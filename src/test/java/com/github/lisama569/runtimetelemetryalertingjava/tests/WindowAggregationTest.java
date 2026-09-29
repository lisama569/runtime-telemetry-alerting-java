package com.github.lisama569.runtimetelemetryalertingjava.tests;

import com.github.lisama569.runtimetelemetryalertingjava.model.AcceptOutcome;
import com.github.lisama569.runtimetelemetryalertingjava.model.AcceptResult;
import com.github.lisama569.runtimetelemetryalertingjava.model.MetricEvent;
import com.github.lisama569.runtimetelemetryalertingjava.model.WindowResult;
import com.github.lisama569.runtimetelemetryalertingjava.windowing.WindowAggregator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 乱序、迟到、重复上报的窗口聚合测试。
 * 失败断言均带独立中文说明，便于区分失败原因。
 */
class WindowAggregationTest {

    private static final Logger log = LoggerFactory.getLogger(WindowAggregationTest.class);

    /** 1s 窗口，500ms 容忍迟到。 */
    private WindowAggregator newAggregator() {
        return new WindowAggregator(1000L, 500L);
    }

    private MetricEvent event(String id, long t, long latency, boolean error) {
        return new MetricEvent(id, "0123456789abcdef0123456789abcdef", t, latency, error);
    }

    private AcceptResult report(WindowAggregator aggregator, MetricEvent e) {
        AcceptResult r = aggregator.accept(e);
        log.info("[输入] eventId={} eventTime={} latency={} error={} => [判定] {} window={} | {}",
                e.eventId(), e.eventTimeMillis(), e.latencyMillis(), e.error(),
                r.outcome(), r.windowStart(), r.reason());
        return r;
    }

    @Test
    @DisplayName("乱序到达：归属窗口与计数不受到达顺序影响")
    void outOfOrderArrivalAggregatesByEventTime() {
        WindowAggregator aggregator = newAggregator();

        // 故意打乱时间顺序上报，全部落在窗口 [0,1000) 与 [1000,2000)
        List<MetricEvent> shuffled = List.of(
                event("e3", 1500, 300, false),
                event("e1", 100, 100, false),
                event("e4", 1900, 400, true),
                event("e2", 900, 200, true),
                event("e5", 100, 500, false)
        );
        for (MetricEvent e : shuffled) {
            AcceptResult r = report(aggregator, e);
            assertEquals(AcceptOutcome.ACCEPTED, r.outcome(),
                    "乱序上报也必须被正常计入，失败于 " + e.eventId());
        }

        // watermark=2500 => horizon=2000，恰好封存 [0,1000)、[1000,2000)
        List<WindowResult> sealed = aggregator.advanceWatermark(2500L);
        sealed.forEach(w -> log.info("[封存窗口] [{},{}) count={} error={} avgLatency={} errorRate={}",
                w.windowStart(), w.windowEnd(), w.count(), w.errorCount(),
                w.averageLatencyMillis(), w.errorRate()));

        assertEquals(2, sealed.size(), "应恰好封存两个窗口，失败于窗口数量");
        WindowResult w0 = sealed.get(0);
        WindowResult w1 = sealed.get(1);

        assertEquals(0L, w0.windowStart(), "第一个窗口起点必须是 0");
        assertEquals(3L, w0.count(), "窗口[0,1000)应有 3 条（e1,e2,e5），乱序不能算错");
        assertEquals(1L, w0.errorCount(), "窗口[0,1000)错误数应为 1（e2）");
        assertEquals(800L, w0.totalLatencyMillis(), "窗口[0,1000)总耗时应为 100+200+500");
        assertEquals(1d / 3, w0.errorRate(), 1e-9, "窗口[0,1000)错误率应为 1/3");

        assertEquals(1000L, w1.windowStart(), "第二个窗口起点必须是 1000");
        assertEquals(2L, w1.count(), "窗口[1000,2000)应有 2 条（e3,e4）");
        assertEquals(1L, w1.errorCount(), "窗口[1000,2000)错误数应为 1（e4）");
        assertEquals(350d, w1.averageLatencyMillis(), 1e-9, "窗口[1000,2000)平均耗时应为 350");
    }

    @Test
    @DisplayName("容忍迟到：封存宽限期内到达仍计入，封存结果含迟到数据")
    void lateWithinAllowedLatenessIsAccepted() {
        WindowAggregator aggregator = newAggregator();
        assertEquals(AcceptOutcome.ACCEPTED, report(aggregator, event("a", 100, 100, false)).outcome(),
                "首条正常计入");

        // watermark=1200 => horizon=700：窗口[0,1000)结束于1000 > 700，尚未封存
        assertTrue(aggregator.advanceWatermark(1200L).isEmpty(),
                "宽限期内不应封存窗口[0,1000)");

        AcceptResult late = report(aggregator, event("late", 500, 300, true));
        assertEquals(AcceptOutcome.ACCEPTED, late.outcome(),
                "在容忍迟到宽限期内（watermark=1200）的 t=500 事件必须能补入窗口");

        // watermark=1500 => horizon=1000：窗口封存，迟到数据已体现在结果中
        List<WindowResult> sealed = aggregator.advanceWatermark(1500L);
        assertEquals(1, sealed.size(), "应封存窗口[0,1000)");
        assertEquals(2L, sealed.get(0).count(), "封存结果必须包含宽限期内补入的迟到数据，共 2 条");
        assertEquals(1L, sealed.get(0).errorCount(), "迟到错误请求也要计入错误数");
        assertEquals(200d, sealed.get(0).averageLatencyMillis(), 1e-9, "平均耗时=(100+300)/2");
    }

    @Test
    @DisplayName("过晚迟到：窗口封存后到达判 TOO_LATE 丢弃，结果不回改")
    void lateAfterSealIsRejected() {
        WindowAggregator aggregator = newAggregator();
        report(aggregator, event("a", 100, 100, false));

        // watermark=1500 => horizon=1000：窗口[0,1000)封存
        List<WindowResult> sealed = aggregator.advanceWatermark(1500L);
        assertEquals(1, sealed.size(), "前置条件：窗口已封存");

        AcceptResult tooLate = report(aggregator, event("late", 200, 999, true));
        assertEquals(AcceptOutcome.TOO_LATE, tooLate.outcome(),
                "封存后到达的事件必须判 TOO_LATE，不能回改已发布结果");
        assertEquals(0L, tooLate.windowStart(), "被丢弃事件仍应给出归属窗口起点，便于解释");

        WindowResult snapshot = sealed.get(0);
        assertEquals(1L, snapshot.count(), "已封存快照计数不能被迟到数据改变");
        assertEquals(0L, snapshot.errorCount(), "已封存快照错误数不能被迟到数据改变");
    }

    @Test
    @DisplayName("重复上报：同一 eventId 无论到达时机都只计一次")
    void duplicateEventIdCountedOnce() {
        WindowAggregator aggregator = newAggregator();
        MetricEvent first = event("dup", 300, 200, false);
        MetricEvent second = new MetricEvent("dup", "other-trace-id", 800, 1, true);

        AcceptResult r1 = report(aggregator, first);
        AcceptResult r2 = report(aggregator, second);
        assertEquals(AcceptOutcome.ACCEPTED, r1.outcome(), "首次上报计入");
        assertEquals(AcceptOutcome.DUPLICATE, r2.outcome(),
                "相同 eventId 第二次上报必须判 DUPLICATE，即使其它字段不同");
        assertEquals(0L, r2.windowStart(), "重复判定应指出首次计入的窗口起点");

        // 窗口封存后再来同一 eventId：仍是 DUPLICATE，而不是 TOO_LATE
        List<WindowResult> sealedAt1500 = aggregator.advanceWatermark(1500L);
        assertEquals(1, sealedAt1500.size(), "前置条件：advance(1500) 封存窗口[0,1000)");
        AcceptResult r3 = report(aggregator, first);
        assertEquals(AcceptOutcome.DUPLICATE, r3.outcome(),
                "封存窗口的重复上报必须识别为 DUPLICATE（幂等表保留），不能误判迟到或重复计数");

        WindowResult w0 = sealedAt1500.get(0);
        assertEquals(0L, w0.windowStart(), "封存窗口起点应为 0");
        assertEquals(1L, w0.count(), "重复上报绝不能把计数算重，窗口只能有 1 条");
        assertEquals(0L, w0.errorCount(), "重复上报的 error 字段不得覆盖首次数据");
    }

    @Test
    @DisplayName("多窗口滚动：封存按时间有序，watermark 不倒退")
    void rollingWindowsSealInOrder() {
        WindowAggregator aggregator = newAggregator();
        report(aggregator, event("w1", 1500, 10, false));
        report(aggregator, event("w0", 500, 20, false));

        List<WindowResult> first = aggregator.advanceWatermark(1600L); // horizon=1100 封存[0,1000)
        assertEquals(1, first.size(), "第一次推进只封存 [0,1000)");
        assertEquals(0L, first.get(0).windowStart(), "先封存的必须是更早的窗口");

        List<WindowResult> back = aggregator.advanceWatermark(100L);
        assertTrue(back.isEmpty(), "watermark 倒退调用不应封存任何窗口");

        List<WindowResult> second = aggregator.advanceWatermark(2500L); // horizon=2000 封存[1000,2000)
        assertEquals(1, second.size(), "第二次推进封存 [1000,2000)");
        assertEquals(1000L, second.get(0).windowStart(), "后封存窗口起点应为 1000");
    }

    @Test
    @DisplayName("空窗口不产生快照：只有收到过数据的窗口才封存")
    void emptyWindowsAreNotSealed() {
        WindowAggregator aggregator = newAggregator();
        report(aggregator, event("only", 5000, 10, false));
        List<WindowResult> sealed = aggregator.advanceWatermark(8000L); // horizon=7500
        assertEquals(1, sealed.size(), "没有数据的空窗口不应产生快照，只封存 [5000,6000)");
        assertEquals(5000L, sealed.get(0).windowStart(), "唯一窗口起点应为 5000");
    }
}
