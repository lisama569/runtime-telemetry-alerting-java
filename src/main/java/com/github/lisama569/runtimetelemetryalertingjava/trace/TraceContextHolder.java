package com.github.lisama569.runtimetelemetryalertingjava.trace;

import org.slf4j.LoggerFactory;

/**
 * 基于 ThreadLocal 的链路上下文持有器。
 *
 * 约定：try-with-resources 风格使用 {@link #ensure} 或手动 set/clear 配对；
 * 线程池场景必须由 TracingExecutor 负责捕获/恢复/清理，避免线程复用导致串链。
 */
public final class TraceContextHolder {

    private static final ThreadLocal<TraceContext> HOLDER = new ThreadLocal<>();

    private TraceContextHolder() {
    }

    public static void set(TraceContext context) {
        if (context == null) {
            clear();
        } else {
            HOLDER.set(context);
        }
    }

    public static TraceContext get() {
        return HOLDER.get();
    }

    /** 当前线程没有上下文时本地新开一条，保证永远能取到合法上下文。 */
    public static TraceContext require() {
        TraceContext ctx = HOLDER.get();
        if (ctx == null) {
            ctx = TraceContext.startNew();
            HOLDER.set(ctx);
            LoggerFactory.getLogger(TraceContextHolder.class)
                    .info("[trace] no context on thread {}, started new traceId={}",
                            Thread.currentThread().getName(), ctx.traceId());
        }
        return ctx;
    }

    public static void clear() {
        HOLDER.remove();
    }
}
