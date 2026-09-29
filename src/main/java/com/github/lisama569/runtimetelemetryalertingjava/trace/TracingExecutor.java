package com.github.lisama569.runtimetelemetryalertingjava.trace;

import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * 对任意 {@link ExecutorService} 的链路透传包装。
 *
 * 传播规则：
 * 1. 提交（submit/execute/invoke*）发生在调用方线程：立刻快照当前 TraceContext（可能为 null）。
 * 2. 任务在池内线程运行前：把快照恢复到工作线程的 ThreadLocal；
 *    若快照为 null，则不设置上下文（绝不把别的任务的上下文留在线程上）。
 * 3. 任务结束（无论成功失败）后：remove 掉工作线程的 ThreadLocal，
 *    线程被复用时不会沿用上一个任务的 traceId —— 不断链、也不串链。
 */
public class TracingExecutor implements ExecutorService, AutoCloseable {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(TracingExecutor.class);

    private final ExecutorService delegate;

    public TracingExecutor(ExecutorService delegate) {
        this.delegate = delegate;
    }

    public static TracingExecutor fixed(int nThreads) {
        return new TracingExecutor(Executors.newFixedThreadPool(nThreads));
    }

    private Runnable wrap(Runnable command) {
        TraceContext captured = TraceContextHolder.get();
        return () -> {
            TraceContext previous = TraceContextHolder.get();
            if (captured != null) {
                TraceContext child = captured.newChildSpan();
                TraceContextHolder.set(child);
                log.info("[trace] hop to worker thread {}: traceId={} spanId={} parent={}",
                        Thread.currentThread().getName(), child.traceId(), child.spanId(), child.parentSpanId());
            }
            try {
                command.run();
            } finally {
                // 恢复提交前工作线程上的状态（通常为 null），杜绝串链
                TraceContextHolder.set(previous);
                if (previous == null) {
                    TraceContextHolder.clear();
                }
            }
        };
    }

    private <T> Callable<T> wrap(Callable<T> task) {
        TraceContext captured = TraceContextHolder.get();
        return () -> {
            TraceContext previous = TraceContextHolder.get();
            if (captured != null) {
                TraceContext child = captured.newChildSpan();
                TraceContextHolder.set(child);
                log.info("[trace] hop to worker thread {}: traceId={} spanId={} parent={}",
                        Thread.currentThread().getName(), child.traceId(), child.spanId(), child.parentSpanId());
            }
            try {
                return task.call();
            } finally {
                TraceContextHolder.set(previous);
                if (previous == null) {
                    TraceContextHolder.clear();
                }
            }
        };
    }

    @Override
    public void execute(Runnable command) {
        delegate.execute(wrap(command));
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return delegate.submit(wrap(task));
    }

    @Override
    public Future<?> submit(Runnable task) {
        return delegate.submit(wrap(task));
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        return delegate.submit(wrap(task), result);
    }

    /** 便捷方法：返回与请求链路同 traceId 的 CompletableFuture（异步执行）。 */
    public <T> CompletableFuture<T> supplyAsync(java.util.function.Supplier<T> supplier) {
        return CompletableFuture.supplyAsync(() -> {
            // wrap 已在 run 前恢复上下文
            return supplier.get();
        }, this);
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
    }

    /** try-with-resources 支持：关闭底层线程池。 */
    @Override
    public void close() {
        delegate.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return delegate.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return delegate.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return delegate.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return delegate.awaitTermination(timeout, unit);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return delegate.invokeAll(wrapAll(tasks));
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException {
        return delegate.invokeAll(wrapAll(tasks), timeout, unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException,
            java.util.concurrent.ExecutionException {
        return delegate.invokeAny(wrapAll(tasks));
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
        return delegate.invokeAny(wrapAll(tasks), timeout, unit);
    }

    private <T> List<Callable<T>> wrapAll(Collection<? extends Callable<T>> tasks) {
        return tasks.stream().map(this::wrap).toList();
    }
}
