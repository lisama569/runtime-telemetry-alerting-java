package com.github.lisama569.runtimetelemetryalertingjava.windowing;

import com.github.lisama569.runtimetelemetryalertingjava.model.AcceptOutcome;
import com.github.lisama569.runtimetelemetryalertingjava.model.AcceptResult;
import com.github.lisama569.runtimetelemetryalertingjava.model.MetricEvent;
import com.github.lisama569.runtimetelemetryalertingjava.model.WindowResult;

import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 固定大小滚动窗口聚合器（本地内存、线程安全）。
 *
 * <h3>窗口模型</h3>
 * <ul>
 *   <li>事件时间（eventTime）决定归属窗口：{@code windowStart = floor(eventTime / size) * size}，
 *       窗口区间为 [windowStart, windowStart + size)。乱序到达不影响归属。</li>
 *   <li>幂等：以 eventId 全局去重，重复上报（无论先到后到）只计一次。</li>
 *   <li>迟到：watermark 随 {@link #advanceWatermark} 单调推进；当
 *       {@code windowEnd <= watermark - allowedLateness} 时窗口被封存（seal），
 *       封存后的窗口不可变，落到封存窗口的迟到数据判 TOO_LATE 丢弃，绝不回改结果。</li>
 *   <li>封存即发布不可变 {@link WindowResult}，结果可解释。</li>
 * </ul>
 */
public final class WindowAggregator {

    private final long windowSizeMillis;
    private final long allowedLatenessMillis;

    /** 打开中的窗口（有数据才创建），按起点有序。 */
    private final TreeMap<Long, MutableWindow> openWindows = new TreeMap<>();
    /** eventId -> 归属窗口起点：全局幂等表，窗口封存后仍保留以识别迟到的重复上报。 */
    private final Map<String, Long> acceptedEventIds = new ConcurrentHashMap<>();
    /** 当前 watermark；null 表示尚未推进。 */
    private Long watermarkMillis;

    public WindowAggregator(long windowSizeMillis, long allowedLatenessMillis) {
        if (windowSizeMillis <= 0) {
            throw new IllegalArgumentException("windowSizeMillis 必须为正数");
        }
        if (allowedLatenessMillis < 0) {
            throw new IllegalArgumentException("allowedLatenessMillis 不能为负");
        }
        this.windowSizeMillis = windowSizeMillis;
        this.allowedLatenessMillis = allowedLatenessMillis;
    }

    public long getWindowSizeMillis() {
        return windowSizeMillis;
    }

    public long getAllowedLatenessMillis() {
        return allowedLatenessMillis;
    }

    /** 接收一条上报，返回带判定依据的处置结论。 */
    public synchronized AcceptResult accept(MetricEvent event) {
        Objects.requireNonNull(event, "event");
        if (event.eventId() == null || event.eventId().isBlank()) {
            throw new IllegalArgumentException("eventId 不能为空，无法幂等去重");
        }
        String eventId = event.eventId();
        long start = Math.floorDiv(event.eventTimeMillis(), windowSizeMillis) * windowSizeMillis;
        long end = start + windowSizeMillis;

        Long existing = acceptedEventIds.get(eventId);
        if (existing != null) {
            return new AcceptResult(AcceptOutcome.DUPLICATE, existing,
                    "重复上报 eventId=" + eventId + "，已在窗口 [" + existing + ","
                            + (existing + windowSizeMillis) + ") 计过一次，本次忽略");
        }

        if (watermarkMillis != null && end <= watermarkMillis - allowedLatenessMillis) {
            return new AcceptResult(AcceptOutcome.TOO_LATE, start,
                    "迟到丢弃 eventId=" + eventId + " eventTime=" + event.eventTimeMillis()
                            + " 归属窗口 [" + start + "," + end + ") 已随 watermark="
                            + watermarkMillis + "（容忍迟到 " + allowedLatenessMillis
                            + "ms）封存，结果不再修改");
        }

        MutableWindow window = openWindows.computeIfAbsent(start, MutableWindow::new);
        window.add(event);
        acceptedEventIds.put(eventId, start);
        return new AcceptResult(AcceptOutcome.ACCEPTED, start,
                "计入窗口 [" + start + "," + end + ") eventId=" + eventId
                        + " latency=" + event.latencyMillis() + "ms error=" + event.error()
                        + "，窗口当前 count=" + window.count);
    }

    /**
     * 推进 watermark：封存所有 {@code windowEnd <= watermark - allowedLateness} 的窗口，
     * 按时间顺序返回新封存窗口的不可变快照。watermark 不允许倒退（倒退调用返回空列表）。
     */
    public synchronized java.util.List<WindowResult> advanceWatermark(long newWatermarkMillis) {
        java.util.List<WindowResult> sealed = new ArrayList<>();
        if (watermarkMillis != null && newWatermarkMillis < watermarkMillis) {
            return sealed;
        }
        watermarkMillis = newWatermarkMillis;
        long horizon = newWatermarkMillis - allowedLatenessMillis;

        var iterator = openWindows.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, MutableWindow> entry = iterator.next();
            MutableWindow window = entry.getValue();
            if (window.start + windowSizeMillis <= horizon) {
                sealed.add(window.toResult());
                iterator.remove();
            } else {
                break; // TreeMap 有序，后面的窗口更晚
            }
        }
        return sealed;
    }

    /** 当前 watermark（未推进时为 {@link Long#MIN_VALUE}）。 */
    public synchronized long currentWatermark() {
        return watermarkMillis == null ? Long.MIN_VALUE : watermarkMillis;
    }

    /** 可变窗口累加器。 */
    private final class MutableWindow {
        private final long start;
        private long count;
        private long errorCount;
        private long totalLatency;

        private MutableWindow(long start) {
            this.start = start;
        }

        private void add(MetricEvent event) {
            count++;
            if (event.error()) {
                errorCount++;
            }
            totalLatency += Math.max(0, event.latencyMillis());
        }

        private WindowResult toResult() {
            return new WindowResult(start, start + windowSizeMillis,
                    count, errorCount, totalLatency);
        }
    }
}
