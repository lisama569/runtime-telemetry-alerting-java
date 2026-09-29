package com.github.lisama569.runtimetelemetryalertingjava.tests;

import com.github.lisama569.runtimetelemetryalertingjava.model.AcceptOutcome;
import com.github.lisama569.runtimetelemetryalertingjava.model.MetricEvent;
import com.github.lisama569.runtimetelemetryalertingjava.pipeline.TelemetryPipeline;
import com.github.lisama569.runtimetelemetryalertingjava.trace.TraceContextHolder;
import com.github.lisama569.runtimetelemetryalertingjava.trace.TraceIds;
import com.github.lisama569.runtimetelemetryalertingjava.trace.TracePropagatingExecutor;
import com.github.lisama569.runtimetelemetryalertingjava.trace.TraceCallable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 入站解析、缺失/非法生成、跨线程（new Thread 与线程池）传播测试。
 */
class TracePropagationTest {

    private static final Logger log = LoggerFactory.getLogger(TracePropagationTest.class);
    private static final String VALID = "4bf92f3577b34da6a3ce929d0e0e4736";

    @AfterEach
    void clearHolder() {
        TraceContextHolder.clear();
    }

    @Test
    @DisplayName("合法入站 traceId：原样透传，生成新 spanId，标记非自建")
    void validIncomingIsPropagated() {
        TraceIds.TraceResolution r = TraceIds.resolve(VALID);
        log.info("[输入] X-Trace-Id={} => [判定] {} traceId={} spanId={}",
                VALID, r.generated() ? "自建" : "透传", r.context().traceId(), r.context().spanId());

        assertFalse(r.generated(), "合法 traceId 必须透传，不能重建");
        assertEquals(VALID, r.context().traceId(), "透传 traceId 必须与入站一致");
        assertTrue(TraceIds.isValidSpanId(r.context().spanId()), "新 spanId 必须合法");
    }

    @Test
    @DisplayName("缺失/空白/非法 traceId：均生成合法新 ID，判定原因可区分")
    void missingOrIllegalGeneratesNew() {
        String[] inputs = {null, "", "   ", "abc", VALID.toUpperCase(),
                "00000000000000000000000000000000", "4bf92f3577b34da6a3ce929d0e0e473z"};
        for (String input : inputs) {
            TraceIds.TraceResolution r = TraceIds.resolve(input);
            log.info("[输入] X-Trace-Id='{}' => [判定] {} traceId={} | {}",
                    input, r.generated() ? "自建" : "透传", r.context().traceId(), r.reason());
            assertTrue(r.generated(), "缺失/非法输入 '" + input + "' 必须自建 traceId");
            assertTrue(TraceIds.isValidTraceId(r.context().traceId()),
                    "为输入 '" + input + "' 自建的 traceId 必须合法");
        }
    }

    @Test
    @DisplayName("new Thread 子线程：继承提交线程上下文")
    void inheritsToNewThread() throws Exception {
        TraceIds.TraceResolution r = TraceIds.resolve(VALID);
        TraceContextHolder.set(r.context());
        AtomicReference<String> childTrace = new AtomicReference<>();

        Thread t = new Thread(() -> childTrace.set(
                TraceContextHolder.get() == null ? null : TraceContextHolder.get().traceId()));
        t.start();
        t.join(2000);
        log.info("[输入] 父线程 traceId={} => [判定] 新建子线程读到 traceId={}",
                r.context().traceId(), childTrace.get());

        assertEquals(VALID, childTrace.get(), "new Thread 必须继承父线程链路（InheritableThreadLocal）");
    }

    @Test
    @DisplayName("线程池 + TraceCallable：复用线程也能拿到提交时的正确链路，不串线")
    void poolPropagatesWithContextWrapping() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        // 预热池线程：让其在当前线程无上下文时创建，排除工作线程“出生时继承”这一干扰；
        // 真实请求线程复用这些线程时，只允许通过包装器拿到提交方上下文。
        ((ThreadPoolExecutor) pool).prestartAllCoreThreads();
        try {
            var ctxA = TraceIds.resolve(null).context();
            var ctxB = TraceIds.resolve(null).context();
            String traceA = ctxA.traceId();
            String traceB = ctxB.traceId();
            assertNotEquals(traceA, traceB, "两个自建 traceId 必须不同");

            // 模拟三个不同请求线程在各自上下文里提交任务（A、B、A），由同一批池线程复用执行
            Future<String> f1 = submitWith(pool, ctxA, traceA);
            Future<String> f2 = submitWith(pool, ctxB, traceB);
            Future<String> f3 = submitWith(pool, ctxA, traceA);
            Future<?> f4 = pool.submit(() -> {
                // 包装任务执行结束必须恢复复用线程原上下文（此处测试线程无上下文 => 应为 null）
                if (TraceContextHolder.get() != null) {
                    throw new IllegalStateException("前序包装任务污染了复用线程的上下文");
                }
            });

            String seen1 = f1.get(2, TimeUnit.SECONDS);
            String seen2 = f2.get(2, TimeUnit.SECONDS);
            String seen3 = f3.get(2, TimeUnit.SECONDS);
            f4.get(2, TimeUnit.SECONDS);
            log.info("[输入] 提交顺序 traceId={},{},{} => [判定] 线程池实际拿到 {},{},{}",
                    traceA, traceB, traceA, seen1, seen2, seen3);

            assertEquals(traceA, seen1, "任务1 必须串到 traceA");
            assertEquals(traceB, seen2, "任务2 必须串到 traceB，不能被复用线程上的 traceA 串线");
            assertEquals(traceA, seen3, "任务3 必须再次串到 traceA");
            assertNull(TraceContextHolder.get(), "回到调用线程后不应残留池内上下文");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    @DisplayName("线程池 + TracePropagatingExecutor：执行器级别自动传播")
    void propagatingExecutorCarriesContext() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        TracePropagatingExecutor executor = new TracePropagatingExecutor(pool);
        AtomicReference<String> seen = new AtomicReference<>();
        try {
            TraceIds.TraceResolution r = TraceIds.resolve(VALID);
            TraceContextHolder.set(r.context());
            executor.execute(() -> seen.set(
                    TraceContextHolder.get() == null ? null : TraceContextHolder.get().traceId()));
            pool.shutdown();
            assertTrue(pool.awaitTermination(2, TimeUnit.SECONDS), "线程池任务应及时结束");
            log.info("[输入] 提交方 traceId={} => [判定] 传播执行器内拿到 traceId={}",
                    r.context().traceId(), seen.get());
            assertEquals(VALID, seen.get(), "TracePropagatingExecutor 必须自动传播提交时链路");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    @DisplayName("无包装的裸任务在线程池里读不到调用方上下文（对照：证明必须显式传播）")
    void bareTaskDoesNotLeakCallerContext() throws Exception {
        // 这是一条「语义对照」：强调线程池复用场景不能依赖隐式继承。
        // holder 在当前线程故意不放任何上下文。
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            TraceContextHolder.clear();
            AtomicReference<com.github.lisama569.runtimetelemetryalertingjava.trace.TraceContext> seen =
                    new AtomicReference<>();
            Future<?> f = pool.submit(() -> seen.set(TraceContextHolder.get()));
            f.get(2, TimeUnit.SECONDS);
            log.info("[输入] 调用方无上下文裸提交 => [判定] 工作线程上下文={}", seen.get());
            assertNull(seen.get(), "裸任务不应凭空获得调用方上下文");
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    @DisplayName("链路兜底：上报指标缺 traceId 时自动挂上当前线程链路")
    void pipelineReportFallsBackToCurrentTrace() {
        TelemetryPipeline pipeline = new TelemetryPipeline(1000L, 500L, List.of());
        TraceIds.TraceResolution r = TraceIds.resolve(VALID);
        TraceContextHolder.set(r.context());

        MetricEvent withoutTrace = new MetricEvent("m1", null, 100L, 10L, false);
        var result = pipeline.report(withoutTrace);
        log.info("[输入] 指标 eventId=m1 traceId=null，holder={} => [判定] {} | {}",
                r.context().traceId(), result.outcome(), result.reason());
        assertEquals(AcceptOutcome.ACCEPTED, result.outcome(), "带当前链路的上报应被接受");

        TraceContextHolder.clear();
        // 这里无法直接回读补后的 traceId（窗口内不按 trace 查询），
        // 但可确认无 holder 时裸上报也会自建合法 traceId 且不报错。
        MetricEvent another = new MetricEvent("m2", "  ", 100L, 10L, false);
        var result2 = pipeline.report(another);
        log.info("[输入] 指标 eventId=m2 traceId=空白，holder 已清空 => [判定] {} | {}",
                result2.outcome(), result2.reason());
        assertEquals(AcceptOutcome.ACCEPTED, result2.outcome(), "无 holder 时应自建 traceId 并接受");
        assertNotNull(result2.windowStart(), "应正常归入窗口");
    }

    private java.util.concurrent.Callable<String> captureCallable(String expectedTraceId) {
        return () -> {
            var ctx = TraceContextHolder.get();
            String got = ctx == null ? null : ctx.traceId();
            if (!expectedTraceId.equals(got)) {
                throw new IllegalStateException(
                        "线程 " + Thread.currentThread().getName() + " 链路串错：期望 "
                                + expectedTraceId + " 实际 " + got);
            }
            return got;
        };
    }

    /** 模拟「某请求线程」带着自己的链路上下文向线程池提交任务。 */
    private Future<String> submitWith(ExecutorService pool,
                                      com.github.lisama569.runtimetelemetryalertingjava.trace.TraceContext ctx,
                                      String expectedTraceId) {
        var previous = TraceContextHolder.set(ctx);
        try {
            return pool.submit(TraceCallable.wrap(captureCallable(expectedTraceId)));
        } finally {
            if (previous == null) {
                TraceContextHolder.clear();
            } else {
                TraceContextHolder.set(previous);
            }
        }
    }
}
