package com.github.lisama569.runtimetelemetryalertingjava.tests;

import com.github.lisama569.runtimetelemetryalertingjava.alert.AlertStateMachine;
import com.github.lisama569.runtimetelemetryalertingjava.model.AlertEvent;
import com.github.lisama569.runtimetelemetryalertingjava.model.AlertRule;
import com.github.lisama569.runtimetelemetryalertingjava.model.ComparisonOperator;
import com.github.lisama569.runtimetelemetryalertingjava.model.MetricPoint;
import com.github.lisama569.runtimetelemetryalertingjava.model.WindowEmit;
import com.github.lisama569.runtimetelemetryalertingjava.model.WindowResultType;
import com.github.lisama569.runtimetelemetryalertingjava.pipeline.MetricsAlertPipeline;
import com.github.lisama569.runtimetelemetryalertingjava.window.WindowAggregator;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 端到端：原始上报（含乱序/迟到/重复） -> 窗口定稿 -> 耗时 & 错误率两条规则各自状态机
 * -> 抖动不触发、连续越限触发、连续恢复恢复。
 */
class PipelineEndToEndTest {

    private static final Logger log = LoggerFactory.getLogger(PipelineEndToEndTest.class);

    private List<WindowEmit> pushWindow(MetricsAlertPipeline p, long windowStart,
                                       double[] latencies, double[] errors) {
        List<WindowEmit> all = new CopyOnWriteArrayList<>();
        for (int i = 0; i < latencies.length; i++) {
            all.addAll(p.report(new MetricPoint("latency", windowStart + 10 * i + 1,
                    latencies[i], "lat-" + windowStart + "-" + i)));
        }
        for (int i = 0; i < errors.length; i++) {
            all.addAll(p.report(new MetricPoint("error-rate", windowStart + 10 * i + 2,
                    errors[i], "err-" + windowStart + "-" + i)));
        }
        // 用一个稍晚的事件把 watermark 推过 windowEnd+lateness(=0)，触发定稿
        all.addAll(p.report(new MetricPoint("clock", windowStart + 1000, 0,
                "clock-" + windowStart)));
        all.forEach(e -> {
            if (e.type() == WindowResultType.ON_TIME) {
                log.info("[e2e] ON_TIME {}", e.detail());
            }
        });
        return all;
    }

    @Test
    void fullPipelineLatencyAndErrorRateWithJitterAndRecovery() {
        WindowAggregator aggregator = new WindowAggregator(1000, 0);
        MetricsAlertPipeline pipeline = new MetricsAlertPipeline(aggregator);

        // 耗时：均值 > 100，2 连触发，2 连恢复
        AlertStateMachine latency = pipeline.registerRule(
                new AlertRule("high-latency", "latency", ComparisonOperator.GT, 100.0, 2, 2));
        // 错误率：均值 >= 0.5，2 连触发，1 连恢复（上报 0=成功 1=错误，均值即错误率）
        AlertStateMachine errors = pipeline.registerRule(
                new AlertRule("error-rate-high", "error-rate", ComparisonOperator.GTE, 0.5, 2, 1));

        List<AlertEvent> latencyEvents = new CopyOnWriteArrayList<>();
        List<AlertEvent> errorEvents = new CopyOnWriteArrayList<>();
        latency.addListener(latencyEvents::add);
        errors.addListener(errorEvents::add);

        // 窗口0：耗时越限(110,130->120)、错误率 1/2=0.5 越限 —— 第 1 个越限窗口，仅 PENDING
        pushWindow(pipeline, 0, new double[]{110, 130}, new double[]{1, 0});
        log.info("[e2e] after w0 latency={} errors={}", latency.state(), errors.state());
        assertTrue(latencyEvents.stream().noneMatch(AlertEvent::triggered),
                "判定依据: 仅 1 个越限窗口，耗时告警不得触发");
        assertTrue(errorEvents.stream().noneMatch(AlertEvent::triggered),
                "判定依据: 仅 1 个越限窗口，错误率告警不得触发");

        // 窗口1000：耗时正常(50,70->60)，抖动打断耗时确认；错误率继续越限(1,1) -> 触发错误率告警
        pushWindow(pipeline, 1000, new double[]{50, 70}, new double[]{1, 1});
        log.info("[e2e] after w1 latency={} errors={}", latency.state(), errors.state());
        assertTrue(latencyEvents.stream().noneMatch(AlertEvent::triggered),
                "判定依据: 第二个耗时窗口正常，抖动必须打断确认，耗时仍零触发");
        assertEquals(1, errorEvents.stream().filter(AlertEvent::triggered).count(),
                "判定依据: 错误率连续 2 窗口越限，必须恰好触发 1 次");

        // 窗口2000、3000：耗时连续越限 -> 触发；错误率窗口3000 恢复
        pushWindow(pipeline, 2000, new double[]{150, 170}, new double[]{0, 1});
        pushWindow(pipeline, 3000, new double[]{200, 100}, new double[]{0, 0});
        log.info("[e2e] after w3 latency={} errors={}", latency.state(), errors.state());
        assertEquals(1, latencyEvents.stream().filter(AlertEvent::triggered).count(),
                "判定依据: 窗口2、3 连续耗时越限，恰好触发 1 次");
        assertEquals(1, errorEvents.stream().filter(AlertEvent::recovered).count(),
                "判定依据: 错误率规则 recoverAfter=1，窗口3 正常立即恢复 1 次");

        // 重复上报窗口0里的某个点：只能得到 DUPLICATE，不影响任何状态
        List<WindowEmit> dup = pipeline.report(new MetricPoint("latency", 11, 110, "lat-0-0"));
        log.info("[e2e] replay duplicate -> {}", dup.stream().map(WindowEmit::type).toList());
        assertEquals(WindowResultType.DUPLICATE, dup.get(0).type(),
                "判定依据: 重放历史点必须被 eventId 去重");
        assertEquals(1, latencyEvents.stream().filter(AlertEvent::triggered).count(),
                "判定依据: 重复上报不能改变任何告警结论");

        // 窗口4000、5000：耗时连续正常 -> 恢复
        pushWindow(pipeline, 4000, new double[]{10, 20}, new double[]{0, 0});
        pushWindow(pipeline, 5000, new double[]{30, 40}, new double[]{0, 0});
        log.info("[e2e] after w5 latency={} errors={}", latency.state(), errors.state());
        assertEquals(1, latencyEvents.stream().filter(AlertEvent::recovered).count(),
                "判定依据: 连续 2 个正常窗口后耗时告警恰好恢复 1 次");
        assertEquals(1, latencyEvents.stream().filter(AlertEvent::triggered).count(),
                "判定依据: 全程耗时告警只触发过 1 次");
        assertEquals(1, errorEvents.stream().filter(AlertEvent::triggered).count(),
                "判定依据: 全程错误率告警只触发过 1 次");

        log.info("[e2e] latency transitions={}", latencyEvents.stream().map(AlertEvent::toState).toList());
        log.info("[e2e] error   transitions={}", errorEvents.stream().map(AlertEvent::toState).toList());
    }
}
