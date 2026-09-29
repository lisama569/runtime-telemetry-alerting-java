package com.github.lisama569.runtimetelemetryalertingjava.pipeline;

import com.github.lisama569.runtimetelemetryalertingjava.model.AlertRule;
import com.github.lisama569.runtimetelemetryalertingjava.model.MetricKind;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * 默认本地内存链路：
 * 1000ms 滚动窗口、500ms 容忍迟到；
 * 平均耗时 > 300ms 连续 2 窗触发、连续 2 窗恢复；
 * 错误率 > 0.5 连续 2 窗触发、连续 2 窗恢复。
 */
@Configuration
public class PipelineConfig {

    public static final long WINDOW_SIZE_MS = 1000L;
    public static final long ALLOWED_LATENESS_MS = 500L;

    @Bean
    public TelemetryPipeline telemetryPipeline() {
        return new TelemetryPipeline(WINDOW_SIZE_MS, ALLOWED_LATENESS_MS, List.of(
                new AlertRule("latency-high", MetricKind.LATENCY_AVG_MS, 300.0d, 2, 2),
                new AlertRule("error-rate-high", MetricKind.ERROR_RATE, 0.5d, 2, 2)
        ));
    }
}
