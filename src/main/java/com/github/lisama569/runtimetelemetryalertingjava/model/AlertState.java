package com.github.lisama569.runtimetelemetryalertingjava.model;

/** 告警状态机的四态。 */
public enum AlertState {
    OK,
    PENDING,
    FIRING,
    RECOVERING
}
