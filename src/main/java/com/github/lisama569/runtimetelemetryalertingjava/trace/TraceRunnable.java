package com.github.lisama569.runtimetelemetryalertingjava.trace;

import java.util.Objects;

/**
 * 在「构造（任务提交）时刻」捕获当前链路上下文，在「执行线程」恢复，
 * 执行结束恢复该线程原上下文的 Runnable 包装器。
 * 专门解决线程池复用线程时 InheritableThreadLocal 不生效/串线的问题。
 */
public final class TraceRunnable implements Runnable {

    private final Runnable delegate;
    private final TraceContext captured;

    private TraceRunnable(Runnable delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.captured = TraceContextHolder.get();
    }

    public static Runnable wrap(Runnable delegate) {
        if (delegate instanceof TraceRunnable) {
            return delegate;
        }
        return new TraceRunnable(delegate);
    }

    @Override
    public void run() {
        TraceContext previous = TraceContextHolder.set(captured);
        try {
            delegate.run();
        } finally {
            if (previous == null) {
                TraceContextHolder.clear();
            } else {
                TraceContextHolder.set(previous);
            }
        }
    }
}
