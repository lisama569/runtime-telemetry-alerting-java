package com.github.lisama569.runtimetelemetryalertingjava.model;

/** 一次原始上报：指标名、事件时间、值、用于去重的事件 ID。 */
public record MetricPoint(String metricName, long eventTimeMillis, double value, String eventId) {
}
