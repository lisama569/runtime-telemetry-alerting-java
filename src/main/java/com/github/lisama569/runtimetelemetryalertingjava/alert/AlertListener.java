package com.github.lisama569.runtimetelemetryalertingjava.alert;

import com.github.lisama569.runtimetelemetryalertingjava.model.AlertEvent;

/** 告警事件监听器。 */
@FunctionalInterface
public interface AlertListener {
    void onAlertEvent(AlertEvent event);
}
