package com.github.lisama569.runtimetelemetryalertingjava.trace;

/**
 * 不可变链路上下文。
 *
 * @param traceId  链路 ID，合法形态为 32 个小写十六进制字符（W3C trace-id 形态）
 * @param spanId   当前 span ID，16 个小写十六进制字符
 * @param sampled  是否采样
 */
public record TraceContext(String traceId, String spanId, boolean sampled) {
}
