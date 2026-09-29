package com.github.lisama569.runtimetelemetryalertingjava.tests;

import com.github.lisama569.runtimetelemetryalertingjava.model.MetricPoint;
import com.github.lisama569.runtimetelemetryalertingjava.model.WindowAggregation;
import com.github.lisama569.runtimetelemetryalertingjava.model.WindowEmit;
import com.github.lisama569.runtimetelemetryalertingjava.model.WindowResultType;
import com.github.lisama569.runtimetelemetryalertingjava.window.WindowAggregator;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 乱序 / 迟到 / 重复上报的窗口聚合测试。
 * 约定：窗口 1000ms，宽限期 500ms，即窗口 [s, s+1000) 在 watermark >= s+1500 时定稿。
 */
class WindowAggregationTest {

    private static final Logger log = LoggerFactory.getLogger(WindowAggregationTest.class);
    private static final long W = 1000;
    private static final long LATENESS = 500;

    @Test
    void orderedPointsAggregateOnTime() {
        WindowAggregator agg = new WindowAggregator(W, LATENESS);
        List<WindowEmit> sink = new java.util.concurrent.CopyOnWriteArrayList<>();
        agg.addListener(sink::add);

        MetricPoint p1 = point("latency", 0, 10, "a");
        MetricPoint p2 = point("latency", 100, 20, "b");
        MetricPoint p3 = point("latency", 200, 30, "c");
        log.info("[case1] ingest {} -> emits={}", p1, types(agg.ingest(p1)));
        log.info("[case1] ingest {} -> emits={}", p2, types(agg.ingest(p2)));
        log.info("[case1] ingest {} -> emits={}", p3, types(agg.ingest(p3)));

        // 触发 watermark=1500，使窗口 [0,1000) 在 end+lateness=1500 处定稿
        List<WindowEmit> closing = agg.advanceWatermark(1500);
        log.info("[case1] advanceWatermark(1500) -> {}", closing.stream().map(WindowEmit::detail).toList());

        WindowEmit onTime = closing.get(0);
        assertEquals(WindowResultType.ON_TIME, onTime.type(), "判定依据: watermark 到达 end+宽限期，应产出 ON_TIME");
        WindowAggregation a = onTime.aggregation();
        assertEquals(3, a.count(), "判定依据: 三个不同 eventId 必须各计一次");
        assertEquals(20.0, a.mean(), 1e-9, "判定依据: (10+20+30)/3 = 20");
        assertEquals(1, sink.stream().filter(e -> e.type() == WindowResultType.ON_TIME).count(),
                "判定依据: 窗口只能定稿一次");
    }

    @Test
    void outOfOrderAndLateUpdatesAreExplainable() {
        WindowAggregator agg = new WindowAggregator(W, LATENESS);

        // 同窗口乱序：先 ts=500 再 ts=100
        List<WindowEmit> e1 = agg.ingest(point("latency", 500, 100, "a"));
        List<WindowEmit> e2 = agg.ingest(point("latency", 100, 200, "b"));
        log.info("[case2] out-of-order ingest ts=500 -> {}; ingest ts=100 -> {}", types(e1), types(e2));
        assertTrue(e2.isEmpty(), "判定依据: 两者都早于 watermark=500 的正常到达，不产生结果，只累加到窗口 [0,1000)");

        // watermark 推进到 1400：窗口 [0,1000) 的定稿点是 1500，尚不定稿
        agg.ingest(point("latency", 1400, 10, "c"));
        // ts=200 落后 watermark(1400)，但窗口还在宽限期内 -> LATE_CORRECTION 并入
        List<WindowEmit> late = agg.ingest(point("latency", 200, 300, "d"));
        log.info("[case2] late ingest ts=200 -> {}", late.stream().map(WindowEmit::detail).toList());
        assertEquals(WindowResultType.LATE_CORRECTION, late.get(0).type(),
                "判定依据: eventTime < watermark 但窗口未过宽限期，必须补算并标记 LATE_CORRECTION");
        assertEquals(3, late.get(0).aggregation().count(), "判定依据: 迟到点补入后窗口计数应为 3");

        // watermark 到达 1500 -> 窗口定稿，迟到点已包含其中
        List<WindowEmit> closed = agg.ingest(point("latency", 1500, 20, "e"));
        log.info("[case2] ingest ts=1500 -> {}", closed.stream().map(WindowEmit::detail).toList());
        WindowEmit onTime = closed.stream().filter(x -> x.type() == WindowResultType.ON_TIME).findFirst()
                .orElseThrow(() -> new AssertionError("判定依据: 窗口 [0,1000) 应在 watermark=1500 定稿"));
        assertEquals(3, onTime.aggregation().count(), "判定依据: 定稿结果必须包含迟到补入的点");
        assertEquals(200.0, onTime.aggregation().mean(), 1e-9, "判定依据: (100+200+300)/3=200");

        // 窗口 [1000,2000) 在 watermark=2600 定稿后，ts=1100 才到 -> DROPPED
        agg.ingest(point("latency", 2600, 9, "f"));
        List<WindowEmit> dropped = agg.ingest(point("latency", 1100, 999, "g"));
        log.info("[case2] late-but-expired ingest ts=1100 -> {}", dropped.stream().map(WindowEmit::detail).toList());
        assertEquals(WindowResultType.DROPPED, dropped.get(0).type(),
                "判定依据: 窗口已定稿且超过宽限期，必须丢弃且不得改动定稿结果");
        assertNull(dropped.get(0).aggregation(), "判定依据: DROPPED 不携带聚合快照");
    }

    @Test
    void duplicatePointsAreCountedOnce() {
        WindowAggregator agg = new WindowAggregator(W, LATENESS);

        MetricPoint dup = point("latency", 0, 10, "dup-1");
        agg.ingest(dup);
        // 完全相同的上报再来一次（模拟重试/重复投递）
        List<WindowEmit> again = agg.ingest(dup);
        log.info("[case3] duplicate ingest {} -> {}", dup, again.stream().map(WindowEmit::detail).toList());
        assertEquals(WindowResultType.DUPLICATE, again.get(0).type(), "判定依据: 相同 eventId 必须识别为重复");
        assertEquals(1, again.get(0).aggregation().count(), "判定依据: 重复上报不能把计数算重");

        // 窗口定稿后，重复事件再次到达：仍是 DUPLICATE 而不是被当成新点或 DROPPED
        List<WindowEmit> closed = agg.advanceWatermark(1500);
        log.info("[case3] close -> {}", closed.stream().map(WindowEmit::detail).toList());
        assertEquals(1, closed.get(0).aggregation().count(), "判定依据: 定稿窗口只含一次事件");
        List<WindowEmit> lateDup = agg.ingest(point("latency", 50, 10, "dup-1"));
        log.info("[case3] duplicate after close -> {}", lateDup.stream().map(WindowEmit::detail).toList());
        assertEquals(WindowResultType.DUPLICATE, lateDup.get(0).type(),
                "判定依据: eventId 全局去重，窗口结束后的重复上报也不得复活窗口");
        assertNull(lateDup.get(0).aggregation(), "判定依据: 重复点不重建已定稿窗口");
        assertTrue(agg.activeWindows().stream().noneMatch(w -> w.metricName().equals("latency")),
                "判定依据: 没有被重复事件重建/污染的活动窗口");
    }

    private static MetricPoint point(String metric, long ts, double v, String id) {
        return new MetricPoint(metric, ts, v, id);
    }

    private static List<WindowResultType> types(List<WindowEmit> emits) {
        return emits.stream().map(WindowEmit::type).toList();
    }
}
