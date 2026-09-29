package com.github.lisama569.runtimetelemetryalertingjava.trace;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** 注册入站链路过滤器。 */
@Configuration
public class TraceWebConfig {

    @Bean
    public InboundTraceFilter inboundTraceFilter() {
        return new InboundTraceFilter();
    }
}
