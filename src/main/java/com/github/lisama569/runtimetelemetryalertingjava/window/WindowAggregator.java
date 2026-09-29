package com.github.lisama569.runtimetelemetryalertingjava.window;

import com.github.lisama569.runtimetelemetryalertingjava.model.MetricPoint;
import com.github.lisama569.runtimetelemetryalertingjava.model.WindowAggregation;
import com.github.lisama569.runtimetelemetryalertingjava.model.WindowEmit;
import com.github.lisama569.runtimetelemetryalertingjava.model.WindowResultType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 固定滚动窗口（tumbling window）聚合器，基于事件时间（event-time）与 watermark。
 *
 * 语义说明：
 * - 窗口按 {@code windowSizeMillis} 对齐：窗口为 [floor(ts/W)*W, +W)。
 * - watermark = 目前见到的最大事件时间，单调不减，代表“认为不会再有更早事件”的断言点。
 * - 宽限期 allowedLateness：窗口在 {@code watermark >= windowEnd + allowedLateness} 时才定稿（ON_TIME）。
 *   到达时其窗口结束时间已被 watermark 越过、但窗口仍在宽限期内的，属于迟到数据，
 *   会被补进窗口并以 LATE_CORRECTION 显式产出；只是在同一个尚未结束的窗口内乱序到达的，
 *   按正常数据静默累加。窗口定稿时所有已接受的（含迟到）数据都包含在结果里。
 * - 窗口定稿之后才到的、归属该窗口的数据一律 DROPPED（不再改动已定稿结果）。
 * - 按 eventId 全局去重：重复上报只会得到 DUPLICATE 结果，绝不二次计数。
 *
 * 所有状态变更都在同一把锁内完成，监听器通知在锁外按顺序发出，保证并发上报下计数一致。
 */
public class WindowAggregator {

    private static final Logger log = LoggerFactory.getLogger(WindowAggregator.class);

    private final long windowSizeMillis;
    private final long allowedLatenessMillis;
    private final List<WindowListener> listeners = new CopyOnWriteArrayList<>();

    /** key: metricName|windowStart */
    private final TreeMap<WindowKey, WindowState> active = new TreeMap<>();
    private final Set<String> seenEventIds = ConcurrentHashMap.newKeySet();
    private long watermark = Long.MIN_VALUE;

    public WindowAggregator(long windowSizeMillis, long allowedLatenessMillis) {
        if (windowSizeMillis <= 0) {
            throw new IllegalArgumentException("windowSizeMillis must be positive");
        }
        if (allowedLatenessMillis < 0) {
            throw new IllegalArgumentException("allowedLatenessMillis must be >= 0");
        }
        this.windowSizeMillis = windowSizeMillis;
        this.allowedLatenessMillis = allowedLatenessMillis;
    }

    public void addListener(WindowListener listener) {
        listeners.add(listener);
    }

    /**
     * 上报一个数据点，返回本次上报引起的全部可解释结果：
     * 可能包含 0..1 条针对该点的 DUPLICATE/LATE_CORRECTION/DROPPED，
     * 以及 0..n 条因 watermark 推进而定稿的 ON_TIME。
     */
    public List<WindowEmit> ingest(MetricPoint point) {
        validate(point);
        List<WindowEmit> emits;
        synchronized (this) {
            emits = doIngest(point);
        }
        emits.forEach(this::publish);
        return emits;
    }

    /** 推进 watermark（无数据时可由定时器/测试驱动），返回定稿窗口的 ON_TIME 结果。 */
    public List<WindowEmit> advanceWatermark(long newWatermark) {
        List<WindowEmit> emits;
        synchronized (this) {
            emits = closeExpired(newWatermark);
        }
        emits.forEach(this::publish);
        return emits;
    }

    public synchronized long watermark() {
        return watermark;
    }

    /** 当前尚未定稿的窗口（按指标、起点排序）。 */
    public synchronized List<WindowAggregation> activeWindows() {
        return active.values().stream().map(s -> s.agg).toList();
    }

    // ---- internals (called under lock) ----

    private List<WindowEmit> doIngest(MetricPoint point) {
        List<WindowEmit> result = new ArrayList<>();
        long start = windowStart(point.eventTimeMillis());
        long end = start + windowSizeMillis;
        WindowKey key = new WindowKey(point.metricName(), start);

        WindowState state = active.get(key);

        if (seenEventIds.contains(point.eventId())) {
            String detail = "DUPLICATE eventId=%s ts=%d ignored, not counted again (window %d~%d)"
                    .formatted(point.eventId(), point.eventTimeMillis(), start, end);
            log.info("[window] {}", detail);
            result.add(new WindowEmit(WindowResultType.DUPLICATE,
                    state == null ? null : state.agg, watermark, detail));
            // 重复事件不推进 watermark，也不可能定稿窗口
            return result;
        }

        if (state == null && watermark >= end + allowedLatenessMillis) {
            String detail = "DROPPED eventId=%s ts=%d, window %d~%d already finalized at watermark=%d"
                    .formatted(point.eventId(), point.eventTimeMillis(), start, end, watermark);
            log.info("[window] {}", detail);
            result.add(new WindowEmit(WindowResultType.DROPPED, null, watermark, detail));
            return result;
        }

        // “迟到”相对窗口结束时间判定：事件时间所在窗口的 end 已被 watermark 越过、
        // 但仍在宽限期内。仅落后于 watermark、而窗口尚未到点的属于窗口内正常乱序，
        // 静默累加即可，避免把乱序误报成迟到。
        boolean late = watermark != Long.MIN_VALUE && watermark >= end;
        if (state == null) {
            state = new WindowState(new WindowAggregation(start, end, point.metricName()));
            active.put(key, state);
        }
        state.agg.add(point);
        if (late) {
            state.lateAccepted++;
        }
        seenEventIds.add(point.eventId());

        if (late) {
            String detail = "LATE_CORRECTION eventId=%s ts=%d for window %d~%d (watermark passed window end, within grace watermark=%d), merged, count now=%d"
                    .formatted(point.eventId(), point.eventTimeMillis(), start, end, watermark, state.agg.count());
            log.info("[window] {}", detail);
            result.add(new WindowEmit(WindowResultType.LATE_CORRECTION, state.agg, watermark, detail));
        }

        result.addAll(closeExpired(point.eventTimeMillis()));
        return result;
    }

    /** 把 watermark 推到 newWatermark，并定稿所有超过宽限期的窗口，返回 ON_TIME 产出。 */
    private List<WindowEmit> closeExpired(long newWatermark) {
        List<WindowEmit> result = new ArrayList<>();
        if (newWatermark <= watermark) {
            return result;
        }
        long previous = watermark;
        watermark = newWatermark;

        var iter = active.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<WindowKey, WindowState> e = iter.next();
            WindowState s = e.getValue();
            long expireAt = s.agg.windowEnd() + allowedLatenessMillis;
            if (watermark >= expireAt) {
                iter.remove();
                String detail = "ON_TIME window %d~%d finalized (watermark %d->%d >= end+lateness %d): count=%d mean=%.4f, lateAccepted=%d"
                        .formatted(s.agg.windowStart(), s.agg.windowEnd(),
                                previous == Long.MIN_VALUE ? 0 : previous, watermark, expireAt,
                                s.agg.count(), s.agg.mean(), s.lateAccepted);
                log.info("[window] {}", detail);
                result.add(new WindowEmit(WindowResultType.ON_TIME, s.agg, watermark, detail));
            }
        }
        return result;
    }

    private long windowStart(long ts) {
        return Math.floorDiv(ts, windowSizeMillis) * windowSizeMillis;
    }

    private void validate(MetricPoint point) {
        if (point == null) {
            throw new IllegalArgumentException("point must not be null");
        }
        if (point.metricName() == null || point.metricName().isBlank()) {
            throw new IllegalArgumentException("metricName must not be blank");
        }
        if (point.eventId() == null || point.eventId().isBlank()) {
            throw new IllegalArgumentException("eventId is required for de-duplication");
        }
        if (point.eventTimeMillis() < 0) {
            throw new IllegalArgumentException("eventTimeMillis must be >= 0");
        }
    }

    private void publish(WindowEmit emit) {
        for (WindowListener l : listeners) {
            l.onWindowEmit(emit);
        }
    }

    private record WindowKey(String metric, long start) implements Comparable<WindowKey> {
        @Override
        public int compareTo(WindowKey o) {
            int c = metric.compareTo(o.metric);
            return c != 0 ? c : Long.compare(start, o.start);
        }
    }

    private static final class WindowState {
        final WindowAggregation agg;
        int lateAccepted;

        WindowState(WindowAggregation agg) {
            this.agg = agg;
        }
    }
}
