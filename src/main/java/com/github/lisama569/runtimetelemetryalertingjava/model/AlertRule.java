package com.github.lisama569.runtimetelemetryalertingjava.model;

/** 告警规则：指标口径、阈值、连续越限/恢复确认窗口数。 */
public final class AlertRule {

    private final String name;
    private final MetricKind metricKind;
    private final double threshold;
    private final int breachConfirmations;
    private final int recoveryConfirmations;

    public AlertRule(String name, MetricKind metricKind, double threshold,
                     int breachConfirmations, int recoveryConfirmations) {
        this.name = name;
        this.metricKind = metricKind;
        this.threshold = threshold;
        this.breachConfirmations = breachConfirmations;
        this.recoveryConfirmations = recoveryConfirmations;
    }

    public String name() {
        return name;
    }

    public MetricKind metricKind() {
        return metricKind;
    }

    public double threshold() {
        return threshold;
    }

    public int breachConfirmations() {
        return breachConfirmations;
    }

    public int recoveryConfirmations() {
        return recoveryConfirmations;
    }

    /** 取该规则关心的指标值。 */
    public double metricValue(WindowResult window) {
        return switch (metricKind) {
            case LATENCY_AVG_MS -> window.averageLatencyMillis();
            case ERROR_RATE -> window.errorRate();
        };
    }

    /** 该窗口是否越限（大于阈值即越限）。空窗口不越限。 */
    public boolean isBreach(WindowResult window) {
        return window.count() > 0 && metricValue(window) > threshold;
    }
}
