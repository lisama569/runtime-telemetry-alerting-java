package com.github.lisama569.runtimetelemetryalertingjava.model;

/** 阈值比较算子。 */
public enum ComparisonOperator {
    GT, GTE, LT, LTE;

    public boolean compare(double value, double threshold) {
        return switch (this) {
            case GT -> value > threshold;
            case GTE -> value >= threshold;
            case LT -> value < threshold;
            case LTE -> value <= threshold;
        };
    }
}
