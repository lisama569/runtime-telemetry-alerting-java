package com.github.lisama569.runtimetelemetryalertingjava.window;

import com.github.lisama569.runtimetelemetryalertingjava.model.WindowEmit;

/** 窗口聚合结果监听器。 */
@FunctionalInterface
public interface WindowListener {
    void onWindowEmit(WindowEmit emit);
}
