package com.github.lisama569.runtimetelemetryalertingjava.pipeline;

import com.github.lisama569.runtimetelemetryalertingjava.alert.AlertStateMachine;
import com.github.lisama569.runtimetelemetryalertingjava.model.ComparisonOperator;
import com.github.lisama569.runtimetelemetryalertingjava.model.AlertRule;
import com.github.lisama569.runtimetelemetryalertingjava.window.WindowAggregator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** 演示用的本地内存流水线 Bean：1s 窗口、0 宽限；耗时与错误率各一条规则。 */
@Configuration
public class DemoPipelineBean {

    @Bean
    public MetricsAlertPipeline metricsAlertPipeline() {
        WindowAggregator aggregator = new WindowAggregator(1000, 0);
        MetricsAlertPipeline pipeline = new MetricsAlertPipeline(aggregator);
        pipeline.registerRule(new AlertRule("high-latency", "latency",
                ComparisonOperator.GT, 100.0, 3, 2));
        pipeline.registerRule(new AlertRule("error-rate-high", "error-rate",
                ComparisonOperator.GTE, 0.5, 3, 2));
        return pipeline;
    }
}
