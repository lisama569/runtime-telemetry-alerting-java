package com.github.lisama569.runtimetelemetryalertingjava.model;

/**
 * 一个滚动窗口被封存时的不可变聚合快照。
 *
 * @param windowStart         窗口起始时间（含）
 * @param windowEnd           窗口结束时间（不含）
 * @param count               请求总数（去重后）
 * @param errorCount          错误请求数
 * @param totalLatencyMillis  耗时总和
 */
public record WindowResult(
        long windowStart,
        long windowEnd,
        long count,
        long errorCount,
        long totalLatencyMillis
) {
    /** 平均耗时（毫秒），空窗口为 0。 */
    public double averageLatencyMillis() {
        return count == 0 ? 0d : (double) totalLatencyMillis / count;
    }

    /** 错误率（0.0 ~ 1.0），空窗口为 0。 */
    public double errorRate() {
        return count == 0 ? 0d : (double) errorCount / count;
    }
}
