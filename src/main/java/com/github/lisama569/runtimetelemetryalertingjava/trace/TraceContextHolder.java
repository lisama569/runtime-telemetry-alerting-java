package com.github.lisama569.runtimetelemetryalertingjava.trace;

/**
 * 基于 {@link InheritableThreadLocal} 的链路上下文持有者。
 *
 * <p>InheritableThreadLocal 只保证「新建」子线程时继承父线程值；线程池复用线程时
 * 继承语义会失效或串线，因此跨线程池必须使用 {@link TraceRunnable}/{@link TraceCallable}
 * 或 {@link TracePropagatingExecutor} 在「提交任务时」捕获、「执行时」恢复。</p>
 */
public final class TraceContextHolder {

    private static final InheritableThreadLocal<TraceContext> HOLDER = new InheritableThreadLocal<>();

    private TraceContextHolder() {
    }

    /** 设置当前线程上下文，返回之前的上下文（可能为 null），便于恢复。 */
    public static TraceContext set(TraceContext context) {
        TraceContext previous = HOLDER.get();
        if (context == null) {
            HOLDER.remove();
        } else {
            HOLDER.set(context);
        }
        return previous;
    }

    public static TraceContext get() {
        return HOLDER.get();
    }

    public static void clear() {
        HOLDER.remove();
    }

    /** 以给定上下文执行：设置 -> 执行 -> 恢复/清理，避免污染复用线程。 */
    public static void runWith(TraceContext context, Runnable action) {
        TraceContext previous = set(context);
        try {
            action.run();
        } finally {
            if (previous == null) {
                clear();
            } else {
                set(previous);
            }
        }
    }
}
