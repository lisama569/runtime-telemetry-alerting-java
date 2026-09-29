package com.github.lisama569.runtimetelemetryalertingjava.model;

/**
 * 告警规则：针对某个指标的窗口聚合值进行阈值比较。
 *
 * @param triggerConfirmations 连续多少个 ON_TIME 窗口越限才触发（>=1）
 * @param recoverConfirmations 连续多少个 ON_TIME 窗口恢复才恢复（>=1）
 */
public record AlertRule(String name,
                        String metricName,
                        ComparisonOperator operator,
                        double threshold,
                        int triggerConfirmations,
                        int recoverConfirmations) {

    public AlertRule {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("rule name must not be blank");
        }
        if (metricName == null || metricName.isBlank()) {
            throw new IllegalArgumentException("metricName must not be blank");
        }
        if (operator == null) {
            throw new IllegalArgumentException("operator must not be null");
        }
        if (triggerConfirmations < 1 || recoverConfirmations < 1) {
            throw new IllegalArgumentException("confirmation counts must be >= 1");
        }
    }

    public boolean isBreaching(double value) {
        return operator.compare(value, threshold);
    }
}
