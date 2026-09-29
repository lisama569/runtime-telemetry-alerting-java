package com.github.lisama569.runtimetelemetryalertingjava.trace;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * 入站链路过滤器：解析 X-Trace-Id —— 合法则透传，缺失/非法则生成合法 ID；
 * 把上下文放入 {@link TraceContextHolder}，并把最终 traceId 写回响应头；
 * 请求结束（含异步派发）清理线程上下文，避免线程复用串线。
 */
public final class InboundTraceFilter extends OncePerRequestFilter {

    public static final String TRACE_HEADER = "X-Trace-Id";

    private static final Logger log = LoggerFactory.getLogger(InboundTraceFilter.class);

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        String incoming = request.getHeader(TRACE_HEADER);
        TraceIds.TraceResolution resolution = TraceIds.resolve(incoming);
        TraceContext context = resolution.context();

        TraceContextHolder.set(context);
        response.setHeader(TRACE_HEADER, context.traceId());
        log.info("入站请求 {} 链路解析: {} | traceId={}, spanId={}",
                request.getRequestURI(), resolution.reason(), context.traceId(), context.spanId());
        try {
            filterChain.doFilter(request, response);
        } finally {
            TraceContextHolder.clear();
        }
    }
}
