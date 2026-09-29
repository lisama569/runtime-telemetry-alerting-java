package com.github.lisama569.runtimetelemetryalertingjava.tests;

import com.github.lisama569.runtimetelemetryalertingjava.trace.TraceContext;
import com.github.lisama569.runtimetelemetryalertingjava.trace.TraceContextHolder;
import com.github.lisama569.runtimetelemetryalertingjava.trace.TraceHeaderFilter;
import com.github.lisama569.runtimetelemetryalertingjava.trace.TraceIds;
import com.github.lisama569.runtimetelemetryalertingjava.trace.TracingExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 链路标识解析、生成与跨线程传播测试。
 */
class TracePropagationTest {

    private static final Logger log = LoggerFactory.getLogger(TracePropagationTest.class);

    @AfterEach
    void cleanThread() {
        TraceContextHolder.clear();
    }

    @Test
    void invalidAndMissingInboundIdsAreReplaced() {
        String valid = "0123456789abcdef0123456789abcdef";
        String tooShort = "abc123";
        String badChar = "g234567890abcdef0123456789abcdef";
        String allZero = "0000000000000000";

        assertEquals(valid, TraceIds.resolveInboundTraceId(valid),
                "判定依据: 合法 32 位十六进制 traceId 必须原样透传（小写规范化）");
        assertEquals(valid, TraceIds.resolveInboundTraceId(valid.toUpperCase()),
                "判定依据: 大写形式应规范化为小写后透传");

        String fromShort = TraceIds.resolveInboundTraceId(tooShort);
        String fromBadChar = TraceIds.resolveInboundTraceId(badChar);
        String fromZero = TraceIds.resolveInboundTraceId(allZero);
        String fromNull = TraceIds.resolveInboundTraceId(null);
        log.info("[trace-case1] tooShort -> {}, badChar -> {}, allZero -> {}, null -> {}",
                fromShort, fromBadChar, fromZero, fromNull);

        for (String generated : List.of(fromShort, fromBadChar, fromZero, fromNull)) {
            assertTrue(TraceIds.isValidTraceId(generated),
                    "判定依据: 缺失/非法入站标识必须被替换为合法 traceId, 实际=" + generated);
        }
        assertEquals(32, fromShort.length(), "判定依据: 新生成的 traceId 为 32 位十六进制");
        assertNotEquals(tooShort, fromShort, "判定依据: 不能把非法值透传下去");

        // require 在无上下文线程上自动起链
        TraceContext auto = TraceContextHolder.require();
        assertTrue(TraceIds.isValidTraceId(auto.traceId()), "判定依据: 线程上无上下文时 require 自动起链");
        assertNull(auto.parentSpanId(), "判定依据: 自动起链的是根 span，无 parent");
    }

    @Test
    void contextPropagatesAcrossThreadPool() throws Exception {
        TraceContext root = TraceContext.inbound("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        TraceContextHolder.set(root);
        log.info("[trace-case2] main thread sets traceId={} spanId={}", root.traceId(), root.spanId());

        try (TracingExecutor pool = TracingExecutor.fixed(4)) {
            List<Future<TraceContext>> futures = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                final int idx = i;
                futures.add(pool.submit(() -> {
                    TraceContext ctx = TraceContextHolder.require();
                    log.info("[trace-case2] task#{} on worker {} got traceId={} spanId={} parent={}",
                            idx, Thread.currentThread().getName(),
                            ctx.traceId(), ctx.spanId(), ctx.parentSpanId());
                    return ctx;
                }));
            }
            Set<String> childSpans = new HashSet<>();
            for (Future<TraceContext> f : futures) {
                TraceContext ctx = f.get();
                assertEquals(root.traceId(), ctx.traceId(),
                        "判定依据: 切到线程池后 traceId 必须保持一致，不能断链");
                assertEquals(root.spanId(), ctx.parentSpanId(),
                        "判定依据: 工作线程的 span 应挂在提交点 span 之下，parent 不能错");
                assertTrue(childSpans.add(ctx.spanId()),
                        "判定依据: 每个任务应开启独立子 span，spanId 不得复用");
            }
        }
    }

    @Test
    void unrelatedTasksDoNotMixContexts() throws Exception {
        try (TracingExecutor pool = TracingExecutor.fixed(2)) {
            // 任务 A 带上下文 tA，任务 B 完全不携带；交错提交到同样 2 个工作线程上
            List<Future<String>> results = new ArrayList<>();
            for (int round = 0; round < 6; round++) {
                TraceContext ctxA = TraceContext.inbound("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
                TraceContextHolder.set(ctxA);
                results.add(pool.submit(() -> TraceContextHolder.require().traceId()));
                TraceContextHolder.clear(); // 模拟另一段没有链路上下文的调用
                results.add(pool.submit(() -> {
                    TraceContext c = TraceContextHolder.get();
                    return c == null ? "<none>" : c.traceId();
                }));
            }
            List<String> got = new ArrayList<>();
            for (Future<String> f : results) {
                got.add(f.get());
            }
            log.info("[trace-case3] interleaved traceIds={}", got);
            for (int i = 0; i < got.size(); i++) {
                if (i % 2 == 0) {
                    assertEquals("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", got.get(i),
                            "判定依据: 带上下文的任务必须取到 tA, idx=" + i);
                } else {
                    assertEquals("<none>", got.get(i),
                            "判定依据: 未携带上下文的任务不能读到上一个任务残留的 traceId（防串链）, idx=" + i);
                }
            }
        }
    }

    @Test
    void filterParsesAndGeneratesTraceId() throws Exception {
        TraceHeaderFilter filter = new TraceHeaderFilter();

        // 合法入站：透传
        MockHttpServletRequest req = new MockHttpServletRequest();
        req.addHeader(TraceHeaderFilter.TRACE_HEADER, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        MockHttpServletResponse resp = new MockHttpServletResponse();
        Set<String> seenInChain = ConcurrentHashMap.newKeySet();
        filter.doFilter(req, resp, (request, response) ->
                seenInChain.add(TraceContextHolder.require().traceId()));
        log.info("[trace-case4] valid inbound response header={}, chainSaw={}",
                resp.getHeader(TraceHeaderFilter.TRACE_HEADER), seenInChain);
        assertEquals("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                resp.getHeader(TraceHeaderFilter.TRACE_HEADER), "判定依据: 合法头必须透传到响应");
        assertTrue(seenInChain.contains("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
                "判定依据: 过滤器之后业务线程必须能取到入站 traceId");
        assertNull(TraceContextHolder.get(), "判定依据: 请求结束必须清理 ThreadLocal");

        // 非法入站：生成
        MockHttpServletRequest bad = new MockHttpServletRequest();
        bad.addHeader(TraceHeaderFilter.TRACE_HEADER, "not-a-trace-id");
        MockHttpServletResponse resp2 = new MockHttpServletResponse();
        filter.doFilter(bad, resp2, (request, response) -> { });
        String generated = resp2.getHeader(TraceHeaderFilter.TRACE_HEADER);
        log.info("[trace-case4] invalid inbound generated header={}, flag={}",
                generated, resp2.getHeader("X-Trace-Id-Generated"));
        assertTrue(TraceIds.isValidTraceId(generated), "判定依据: 非法入站头必须被替换为合法生成值");
        assertFalse(generated.equals("not-a-trace-id"), "判定依据: 非法值不得透传");
        assertEquals("true", resp2.getHeader("X-Trace-Id-Generated"),
                "判定依据: 服务端生成时必须显式打标，便于调用方区分");
    }
}
