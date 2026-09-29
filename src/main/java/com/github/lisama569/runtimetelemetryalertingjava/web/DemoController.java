package com.github.lisama569.runtimetelemetryalertingjava.web;

import com.github.lisama569.runtimetelemetryalertingjava.trace.TraceContext;
import com.github.lisama569.runtimetelemetryalertingjava.trace.TraceContextHolder;
import com.github.lisama569.runtimetelemetryalertingjava.trace.TracingExecutor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * 本地手工验证链路透传的演示端点：
 * 同步 / 同步 / 异步两种场景都会回显当前线程与上下文中的 traceId/spanId。
 */
@RestController
public class DemoController {

    private final TracingExecutor executor = TracingExecutor.fixed(2);

    @GetMapping("/demo/sync")
    public Map<String, String> sync() {
        TraceContext ctx = TraceContextHolder.require();
        return snapshot("sync-handler", ctx);
    }

    @GetMapping("/demo/async")
    public CompletableFuture<Map<String, String>> async() {
        TraceContext parent = TraceContextHolder.require();
        return executor.supplyAsync(() -> snapshot("async-worker", TraceContextHolder.require()));
    }

    private Map<String, String> snapshot(String hop, TraceContext ctx) {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("hop", hop);
        m.put("thread", Thread.currentThread().getName());
        m.put("traceId", ctx.traceId());
        m.put("spanId", ctx.spanId());
        m.put("parentSpanId", ctx.parentSpanId() == null ? "" : ctx.parentSpanId());
        return m;
    }
}
