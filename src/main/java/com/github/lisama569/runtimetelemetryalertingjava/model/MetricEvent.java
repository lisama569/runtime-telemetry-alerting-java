package com.github.lisama569.runtimetelemetryalertingjava.model;

/**
 * 一条请求指标上报。
 *
 * @param eventId        上报唯一标识，用于幂等去重
 * @param traceId        链路标识
 * @param eventTimeMillis 事件实际发生时间（由调用方给时间戳，便于模拟乱序/迟到）
 * @param latencyMillis  请求耗时
 * @param error          是否为错误请求
 */
public record MetricEvent(
        String eventId,
        String traceId,
        long eventTimeMillis,
        long latencyMillis,
        boolean error
) {
}
