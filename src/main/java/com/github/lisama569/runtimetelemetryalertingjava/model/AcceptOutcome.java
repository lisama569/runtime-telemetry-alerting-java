package com.github.lisama569.runtimetelemetryalertingjava.model;

/** 单条上报进入聚合器的处置结果。 */
public enum AcceptOutcome {
    /** 正常计入窗口。 */
    ACCEPTED,
    /** 重复上报，已忽略。 */
    DUPLICATE,
    /** 超过容忍延迟、窗口已封存，已丢弃。 */
    TOO_LATE
}
