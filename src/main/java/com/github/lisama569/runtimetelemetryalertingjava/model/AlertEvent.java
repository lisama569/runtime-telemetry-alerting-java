package com.github.lisama569.runtimetelemetryalertingjava.model;

/**
 * 告警状态机发生状态转换时产出的事件。
 *
 * @param kind 事件性质：FIRED=真正触发，RECOVERED=真正恢复，STATE_CHANGE=中间态迁移（审计用）
 */
public record AlertEvent(Kind kind,
                         String ruleName,
                         AlertState fromState,
                         AlertState toState,
                         double observedValue,
                         long windowStart,
                         long windowEnd,
                         String reason,
                         long eventTimeMillis) {

    public enum Kind {
        FIRED,
        RECOVERED,
        STATE_CHANGE
    }

    /** 是否为一次真正的告警触发（RECOVERING 回退到 FIRING 不算）。 */
    public boolean triggered() {
        return kind == Kind.FIRED;
    }

    /** 是否为一次真正的恢复。 */
    public boolean recovered() {
        return kind == Kind.RECOVERED;
    }
}
