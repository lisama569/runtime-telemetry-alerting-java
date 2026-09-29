package com.github.lisama569.runtimetelemetryalertingjava.model;

/**
 * 告警状态机的四个状态：
 * NORMAL（正常）-> PENDING（连续越限确认中）-> FIRING（已触发）
 * FIRING -> RESOLVING（连续恢复确认中）-> NORMAL
 */
public enum AlertState {
    NORMAL,
    PENDING,
    FIRING,
    RESOLVING
}
