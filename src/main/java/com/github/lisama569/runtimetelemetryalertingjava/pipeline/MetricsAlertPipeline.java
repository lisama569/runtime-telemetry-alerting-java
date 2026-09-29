package com.github.lisama569.runtimetelemetryalertingjava.pipeline;

import com.github.lisama569.runtimetelemetryalertingjava.alert.AlertStateMachine;
import com.github.lisama569.runtimetelemetryalertingjava.model.AlertRule;
import com.github.lisama569.runtimetelemetryalertingjava.model.MetricPoint;
import com.github.lisama569.runtimetelemetryalertingjava.model.WindowEmit;
import com.github.lisama569.runtimetelemetryalertingjava.window.WindowAggregator;
import com.github.lisama569.runtimetelemetryalertingjava.window.WindowListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 串联原始上报 -> 窗口聚合 -> 按指标把 ON_TIME 结果分发给各告警状态机。
 *
 * 迟到修正（LATE_CORRECTION）不会重复驱动状态机：告警只以窗口定稿时的
 * 第一手 ON_TIME 结果为准，避免迟到数据让告警来回抖动；
 * 但修正后的聚合结果仍会通过窗口监听器输出，可用于展示/审计。
 */
public class MetricsAlertPipeline implements WindowListener {

    private static final Logger log = LoggerFactory.getLogger(MetricsAlertPipeline.class);

    private final WindowAggregator aggregator;
    private final List<AlertStateMachine> machines = new CopyOnWriteArrayList<>();

    public MetricsAlertPipeline(WindowAggregator aggregator) {
        this.aggregator = aggregator;
        aggregator.addListener(this);
    }

    public AlertStateMachine registerRule(AlertRule rule) {
        AlertStateMachine machine = new AlertStateMachine(rule);
        machines.add(machine);
        log.info("[pipeline] registered alert rule '{}' on metric '{}' {} {} triggerAfter={} recoverAfter={}",
                rule.name(), rule.metricName(), rule.operator(), rule.threshold(),
                rule.triggerConfirmations(), rule.recoverConfirmations());
        return machine;
    }

    /** 上报一个原始数据点，返回聚合器产出的可解释结果。 */
    public List<WindowEmit> report(MetricPoint point) {
        return aggregator.ingest(point);
    }

    public List<WindowEmit> advanceWatermark(long watermark) {
        return aggregator.advanceWatermark(watermark);
    }

    public List<AlertStateMachine> machines() {
        return List.copyOf(machines);
    }

    @Override
    public void onWindowEmit(WindowEmit emit) {
        for (AlertStateMachine m : machines) {
            m.evaluate(emit);
        }
    }
}
