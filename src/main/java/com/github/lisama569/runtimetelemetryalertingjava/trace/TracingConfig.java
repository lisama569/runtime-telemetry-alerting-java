package com.github.lisama569.runtimetelemetryalertingjava.trace;

import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** 注册入站链路过滤器。 */
@Configuration
public class TracingConfig {

    @Bean
    public FilterRegistrationBean<TraceHeaderFilter> traceHeaderFilterRegistration() {
        FilterRegistrationBean<TraceHeaderFilter> registration = new FilterRegistrationBean<>();
        registration.setFilter(new TraceHeaderFilter());
        registration.addUrlPatterns("/*");
        registration.setOrder(1);
        registration.setName("traceHeaderFilter");
        return registration;
    }
}
