package com.github.lisama569.runtimetelemetryalertingjava.web;

import com.github.lisama569.runtimetelemetryalertingjava.model.AcceptResult;
import com.github.lisama569.runtimetelemetryalertingjava.model.AlertEvent;
import com.github.lisama569.runtimetelemetryalertingjava.model.MetricEvent;
import com.github.lisama569.runtimetelemetryalertingjava.pipeline.TelemetryPipeline;
import com.github.lisama569.runtimetelemetryalertingjava.trace.TraceContext;
import com.github.lisama569.runtimetelemetryalertingjava.trace.TraceContextHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 本地演示接口：上报指标、推进 watermark、查询封存窗口/告警事件、回显当前链路。 */
@RestController
@RequestMapping("/telemetry")
public class TelemetryController {

    private static final Logger log = LoggerFactory.getLogger(TelemetryController.class);

    private final TelemetryPipeline pipeline;

    public TelemetryController(TelemetryPipeline pipeline) {
        this.pipeline = pipeline;
    }

    /** body: {eventId, eventTime?, latencyMillis?, error?, traceId?}；traceId 缺省取当前链路。 */
    @PostMapping("/events")
    public Map<String, Object> report(@RequestBody Map<String, Object> body) {
        String eventId = str(body.get("eventId"));
        long eventTime = body.get("eventTime") instanceof Number n
                ? n.longValue() : System.currentTimeMillis();
        long latency = body.get("latencyMillis") instanceof Number n ? n.longValue() : 0L;
        boolean error = Boolean.TRUE.equals(body.get("error"));
        String traceId = str(body.get("traceId"));
        if (traceId == null) {
            TraceContext ctx = TraceContextHolder.get();
            traceId = ctx == null ? null : ctx.traceId();
        }

        MetricEvent event = new MetricEvent(eventId, traceId, eventTime, latency, error);
        log.info("收到上报输入: event={}, 当前holder.traceId={}", event,
                TraceContextHolder.get() == null ? null : TraceContextHolder.get().traceId());
        AcceptResult result = pipeline.report(event);
        log.info("聚合判定依据: outcome={}, windowStart={}, reason={}",
                result.outcome(), result.windowStart(), result.reason());

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("eventId", eventId);
        response.put("traceId", event.traceId());
        response.put("outcome", result.outcome().name());
        response.put("windowStart", result.windowStart());
        response.put("reason", result.reason());
        return response;
    }

    /** body: {watermark}；推进封存并返回本次告警事件。 */
    @PostMapping("/advance")
    public Map<String, Object> advance(@RequestBody Map<String, Object> body) {
        long watermark = body.get("watermark") instanceof Number n
                ? n.longValue() : System.currentTimeMillis();
        log.info("推进 watermark 输入: {}", watermark);
        List<AlertEvent> fired = pipeline.advance(watermark);
        for (AlertEvent event : fired) {
            log.info("告警状态跳变: rule={} {}->{} @{} 依据: {}",
                    event.ruleName(), event.from(), event.to(), event.timestampMillis(),
                    event.reason());
        }
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("watermark", pipeline.currentWatermark());
        response.put("newAlertEvents", fired);
        return response;
    }

    @GetMapping("/windows")
    public Object windows() {
        return pipeline.sealedWindows();
    }

    @GetMapping("/alerts")
    public Object alerts() {
        return Map.of(
                "events", pipeline.alertEvents(),
                "latency-high", String.valueOf(pipeline.alertState("latency-high")),
                "error-rate-high", String.valueOf(pipeline.alertState("error-rate-high"))
        );
    }

    @GetMapping("/trace")
    public Map<String, Object> trace() {
        TraceContext ctx = TraceContextHolder.get();
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("traceId", ctx == null ? null : ctx.traceId());
        response.put("spanId", ctx == null ? null : ctx.spanId());
        return response;
    }

    private static String str(Object value) {
        if (value == null) {
            return null;
        }
        String s = String.valueOf(value).trim();
        return s.isEmpty() ? null : s;
    }
}
