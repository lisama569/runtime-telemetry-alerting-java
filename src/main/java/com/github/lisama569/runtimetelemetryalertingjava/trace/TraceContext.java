package com.github.lisama569.runtimetelemetryalertingjava.trace;

/**
 * 一次调用链上下文。
 *
 * @param traceId      整条链路共享的标识
 * @param spanId       当前这一跳的 span 标识
 * @param parentSpanId 上一跳 span 标识；链路入口为 null
 * @param inbound      是否由入站请求解析而来（false 表示本地生成）
 */
public record TraceContext(String traceId, String spanId, String parentSpanId, boolean inbound) {

    public TraceContext {
        if (!TraceIds.isValidTraceId(traceId)) {
            throw new IllegalArgumentException("illegal traceId: " + traceId);
        }
        if (!TraceIds.isValidSpanId(spanId)) {
            throw new IllegalArgumentException("illegal spanId: " + spanId);
        }
    }

    /** 本地新开一条链路：生成新的 traceId 与根 span。 */
    public static TraceContext startNew() {
        return new TraceContext(TraceIds.generateTraceId(), TraceIds.generateSpanId(), null, false);
    }

    /**
     * 基于入站头创建入口上下文（根 span）。
     *
     * @param inbound 入站请求携带的 traceId；合法则透传（inbound=true），否则生成（inbound=false）
     */
    public static TraceContext inbound(String inbound) {
        boolean headerValid = TraceIds.isValidTraceId(inbound);
        return new TraceContext(TraceIds.resolveInboundTraceId(inbound),
                TraceIds.generateSpanId(), null, headerValid);
    }

    /** 在本上下文下开启一个新的子 span：traceId 不变，parent 指向当前 span。 */
    public TraceContext newChildSpan() {
        return new TraceContext(traceId, TraceIds.generateSpanId(), spanId, inbound);
    }
}
