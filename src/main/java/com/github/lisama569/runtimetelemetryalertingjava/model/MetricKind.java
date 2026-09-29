package com.github.lisama569.runtimetelemetryalertingjava.model;

/** 告警规则关心的指标口径。 */
public enum MetricKind {
    /** 窗口内平均请求耗时（毫秒）。 */
    LATENCY_AVG_MS,
    /** 窗口内错误率（0.0 ~ 1.0）。 */
    ERROR_RATE
}
