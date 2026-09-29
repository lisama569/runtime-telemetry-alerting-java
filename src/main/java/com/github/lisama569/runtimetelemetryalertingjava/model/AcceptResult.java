package com.github.lisama569.runtimetelemetryalertingjava.model;

/**
 * 聚合器对一条上报的判定结论（可解释性用）。
 *
 * @param outcome     处置类型
 * @param windowStart 归属窗口起点；被丢弃时可能为 null
 * @param reason      文字判定依据
 */
public record AcceptResult(AcceptOutcome outcome, Long windowStart, String reason) {
}
