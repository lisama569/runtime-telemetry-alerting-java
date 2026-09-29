package com.github.lisama569.runtimetelemetryalertingjava.model;

/**
 * 告警状态发生跳变时产生的事件（只有跳变才产生，抖动不产生）。
 *
 * @param ruleName        规则名
 * @param from            跳变前状态
 * @param to              跳变后状态
 * @param timestampMillis 判定时间（以触发判定的窗口时间为准）
 * @param window          引起跳变的窗口快照
 * @param reason          判定依据描述
 */
public record AlertEvent(
        String ruleName,
        AlertState from,
        AlertState to,
        long timestampMillis,
        WindowResult window,
        String reason
) {
}
