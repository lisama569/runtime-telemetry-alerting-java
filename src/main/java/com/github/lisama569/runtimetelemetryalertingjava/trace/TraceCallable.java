package com.github.lisama569.runtimetelemetryalertingjava.trace;

import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * 在「构造（任务提交）时刻」捕获当前链路上下文，在「执行线程」恢复，
 * 执行结束恢复该线程原上下文的 Callable 包装器。
 */
public final class TraceCallable<T> implements Callable<T> {

    private final Callable<T> delegate;
    private final TraceContext captured;

    private TraceCallable(Callable<T> delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.captured = TraceContextHolder.get();
    }

    public static <T> Callable<T> wrap(Callable<T> delegate) {
        if (delegate instanceof TraceCallable) {
            return delegate;
        }
        return new TraceCallable<>(delegate);
    }

    @Override
    public T call() throws Exception {
        TraceContext previous = TraceContextHolder.set(captured);
        try {
            return delegate.call();
        } finally {
            if (previous == null) {
                TraceContextHolder.clear();
            } else {
                TraceContextHolder.set(previous);
            }
        }
    }
}
