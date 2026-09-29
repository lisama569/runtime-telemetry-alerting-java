package com.github.lisama569.runtimetelemetryalertingjava.trace;

import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * 把每个任务在「提交时刻」包装为携带当时链路上下文的任务，
 * 再交给被代理的执行器。线程池复用线程也能正确串起链路。
 */
public final class TracePropagatingExecutor implements Executor {

    private final Executor delegate;

    public TracePropagatingExecutor(Executor delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    @Override
    public void execute(Runnable command) {
        delegate.execute(TraceRunnable.wrap(command));
    }
}
