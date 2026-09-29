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
 * 入站链路过滤器。
 *
 * - 读取请求头 X-Trace-Id；合法（16~32 位非全 0 十六进制）则原样透传（小写规范化）。
 * - 缺失 / 非法则生成新的合法 traceId。
 * - 解析结果放进当前请求线程的 {@link TraceContextHolder}，并回写响应头，
 *   请求处理结束后清理 ThreadLocal。
 */
public class TraceHeaderFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(TraceHeaderFilter.class);

    public static final String TRACE_HEADER = "X-Trace-Id";

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String inbound = request.getHeader(TRACE_HEADER);
        boolean headerValid = TraceIds.isValidTraceId(inbound);
        TraceContext context = TraceContext.inbound(inbound);
        TraceContextHolder.set(context);
        if (!headerValid) {
            response.setHeader("X-Trace-Id-Generated", "true");
        }
        response.setHeader(TRACE_HEADER, context.traceId());
        log.info("[trace] inbound request '{} {}' headerTraceId='{}' resolvedTraceId={} rootSpanId={} generated={}",
                request.getMethod(), request.getRequestURI(), inbound,
                context.traceId(), context.spanId(), !headerValid);
        try {
            chain.doFilter(request, response);
        } finally {
            TraceContextHolder.clear();
        }
    }
}
