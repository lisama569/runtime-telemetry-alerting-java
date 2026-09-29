package com.github.lisama569.runtimetelemetryalertingjava.web;

import com.github.lisama569.runtimetelemetryalertingjava.model.MetricPoint;
import com.github.lisama569.runtimetelemetryalertingjava.model.WindowAggregation;
import com.github.lisama569.runtimetelemetryalertingjava.model.WindowEmit;
import com.github.lisama569.runtimetelemetryalertingjava.pipeline.MetricsAlertPipeline;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 本地手工验证采集/聚合/告警的端点（内存模拟，无外部依赖）：
 * - POST /api/metrics/report?metric=&value=&eventTime=&eventId= 上报一个点
 * - POST /api/metrics/advance?watermark= 手动推进 watermark，定稿窗口并驱动告警
 * - GET  /api/alerts/status 查看各规则状态与已发生事件
 */
@RestController
public class TelemetryController {

    private final MetricsAlertPipeline pipeline;

    public TelemetryController(MetricsAlertPipeline pipeline) {
        this.pipeline = pipeline;
    }

    @PostMapping("/api/metrics/report")
    public Map<String, Object> report(@RequestParam String metric,
                                      @RequestParam double value,
                                      @RequestParam(name = "eventTime") long eventTimeMillis,
                                      @RequestParam String eventId) {
        List<WindowEmit> emits = pipeline.report(new MetricPoint(metric, eventTimeMillis, value, eventId));
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("accepted", true);
        body.put("emits", emits.stream().map(this::emitView).toList());
        return body;
    }

    @PostMapping("/api/metrics/advance")
    public Map<String, Object> advance(@RequestParam long watermark) {
        List<WindowEmit> emits = pipeline.advanceWatermark(watermark);
        return Map.of("watermark", watermark, "emits", emits.stream().map(this::emitView).toList());
    }

    @GetMapping("/api/alerts/status")
    public Map<String, Object> status() {
        return Map.of("rules", pipeline.machines().stream().map(m -> {
            Map<String, Object> v = new LinkedHashMap<>();
            v.put("rule", m.rule().name());
            v.put("metric", m.rule().metricName());
            v.put("state", m.state());
            v.put("breachStreak", m.breachStreak());
            v.put("okStreak", m.okStreak());
            v.put("evaluatedWindows", m.evaluatedWindowCount());
            v.put("events", m.events());
            return v;
        }).toList());
    }

    private Map<String, Object> emitView(WindowEmit e) {
        Map<String, Object> v = new LinkedHashMap<>();
        v.put("type", e.type());
        v.put("watermark", e.watermarkMillis());
        v.put("detail", e.detail());
        WindowAggregation a = e.aggregation();
        if (a != null) {
            Map<String, Object> agg = new LinkedHashMap<>();
            agg.put("metric", a.metricName());
            agg.put("windowStart", a.windowStart());
            agg.put("windowEnd", a.windowEnd());
            agg.put("count", a.count());
            agg.put("mean", a.mean());
            agg.put("min", a.min());
            agg.put("max", a.max());
            v.put("aggregation", agg);
        }
        return v;
    }
}
