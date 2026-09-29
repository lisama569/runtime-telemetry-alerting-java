package com.github.lisama569.runtimetelemetryalertingjava.pipeline;

import com.github.lisama569.runtimetelemetryalertingjava.alert.AlertEngine;
import com.github.lisama569.runtimetelemetryalertingjava.model.AcceptResult;
import com.github.lisama569.runtimetelemetryalertingjava.model.AlertEvent;
import com.github.lisama569.runtimetelemetryalertingjava.model.AlertRule;
import com.github.lisama569.runtimetelemetryalertingjava.model.AlertState;
import com.github.lisama569.runtimetelemetryalertingjava.model.MetricEvent;
import com.github.lisama569.runtimetelemetryalertingjava.model.WindowResult;
import com.github.lisama569.runtimetelemetryalertingjava.trace.TraceContext;
import com.github.lisama569.runtimetelemetryalertingjava.trace.TraceContextHolder;
import com.github.lisama569.runtimetelemetryalertingjava.trace.TraceIds;
import com.github.lisama569.runtimetelemetryalertingjava.windowing.WindowAggregator;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 采集 -> 窗口聚合 -> watermark 封存 -> 告警状态机判定的本地内存链路门面。
 *
 * <p>线程安全：上报走聚合器内部锁；{@link #advance} 用本对象锁串行化
 * 「封存 + 告警判定」，保证并发上报下每个封存窗口恰好被每个规则评估一次，
 * 状态转换一致——不重复触发、不漏触发。</p>
 */
public final class TelemetryPipeline {

    private final WindowAggregator aggregator;
    private final AlertEngine alertEngine;

    private final List<WindowResult> sealedWindows = new ArrayList<>();
    private final List<AlertEvent> alertEvents = new ArrayList<>();

    public TelemetryPipeline(long windowSizeMillis, long allowedLatenessMillis,
                             List<AlertRule> rules) {
        this.aggregator = new WindowAggregator(windowSizeMillis, allowedLatenessMillis);
        this.alertEngine = new AlertEngine(Objects.requireNonNull(rules, "rules"));
    }

    /** 上报一条指标；traceId 缺失时用当前链路上下文兜底，再没有则生成。 */
    public AcceptResult report(MetricEvent event) {
        Objects.requireNonNull(event, "event");
        MetricEvent effective = event;
        if (event.traceId() == null || event.traceId().isBlank()) {
            TraceContext current = TraceContextHolder.get();
            String traceId = current != null ? current.traceId() : TraceIds.generateTraceId();
            effective = new MetricEvent(event.eventId(), traceId, event.eventTimeMillis(),
                    event.latencyMillis(), event.error());
        }
        return aggregator.accept(effective);
    }

    /** 推进 watermark，封存到期窗口并逐条过告警规则，返回本次新产生的告警跳变事件。 */
    public synchronized List<AlertEvent> advance(long watermarkMillis) {
        List<WindowResult> newlySealed = aggregator.advanceWatermark(watermarkMillis);
        List<AlertEvent> fired = new ArrayList<>();
        for (WindowResult window : newlySealed) {
            sealedWindows.add(window);
            fired.addAll(alertEngine.evaluate(window));
        }
        alertEvents.addAll(fired);
        return fired;
    }

    public long currentWatermark() {
        return aggregator.currentWatermark();
    }

    public synchronized List<WindowResult> sealedWindows() {
        return List.copyOf(sealedWindows);
    }

    public AlertState alertState(String ruleName) {
        return alertEngine.stateOf(ruleName);
    }

    public synchronized List<AlertEvent> alertEvents() {
        return List.copyOf(alertEvents);
    }
}
