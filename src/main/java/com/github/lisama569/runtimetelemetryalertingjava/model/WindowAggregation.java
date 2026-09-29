package com.github.lisama569.runtimetelemetryalertingjava.model;

/**
 * 单个时间窗口内某指标的可变累加器。
 * 窗口内按 count/sum/min/max 维护，均值 mean = sum/count。
 * 注：错误率类指标可由调用方按约定上报（如 0.0/1.0 或百分比），
 * 聚合器只做无差别的数值聚合，count 始终等于参与聚合的去重事件数。
 */
public class WindowAggregation {
    private final long windowStart;
    private final long windowEnd;
    private final String metricName;
    private long count;
    private double sum;
    private double min = Double.POSITIVE_INFINITY;
    private double max = Double.NEGATIVE_INFINITY;

    public WindowAggregation(long windowStart, long windowEnd, String metricName) {
        if (windowEnd <= windowStart) {
            throw new IllegalArgumentException("windowEnd must be greater than windowStart");
        }
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.metricName = metricName;
    }

    /** 纳入一个（已去重的）数据点。 */
    public void add(MetricPoint point) {
        count++;
        sum += point.value();
        min = Math.min(min, point.value());
        max = Math.max(max, point.value());
    }

    /** 窗口均值；空窗口返回 0（空窗口正常不会被产出）。 */
    public double mean() {
        return count == 0 ? 0d : sum / count;
    }

    /** 越限判定常用的“值”：这里取均值。 */
    public double observedValue() {
        return mean();
    }

    public long windowStart() { return windowStart; }
    public long windowEnd() { return windowEnd; }
    public String metricName() { return metricName; }
    public long count() { return count; }
    public double sum() { return sum; }
    public double min() { return count == 0 ? 0d : min; }
    public double max() { return count == 0 ? 0d : max; }

    @Override
    public String toString() {
        return "Window[%s, %d~%d) count=%d mean=%.4f min=%.4f max=%.4f"
                .formatted(metricName, windowStart, windowEnd, count, mean(), min(), max());
    }
}
