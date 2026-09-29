package com.github.lisama569.runtimetelemetryalertingjava.model;

/** 聚合器对外产出的一条可解释结果。 */
public record WindowEmit(WindowResultType type,
                         WindowAggregation aggregation,
                         long watermarkMillis,
                         String detail) {
}
