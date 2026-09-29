package com.github.lisama569.runtimetelemetryalertingjava.trace;

import java.security.SecureRandom;

/** traceId / spanId 的生成与合法性校验（W3C trace context 形态）。 */
public final class TraceIds {

    /** 32 位小写十六进制。 */
    public static final int TRACE_ID_HEX_LENGTH = 32;
    /** 16 位小写十六进制。 */
    public static final int SPAN_ID_HEX_LENGTH = 16;

    private static final SecureRandom RANDOM = new SecureRandom();
    private static final String HEX = "0123456789abcdef";

    private TraceIds() {
    }

    /** 合法 traceId：32 位小写十六进制，且不能全 0。 */
    public static boolean isValidTraceId(String value) {
        return isLowerHex(value, TRACE_ID_HEX_LENGTH) && !"0".repeat(TRACE_ID_HEX_LENGTH).equals(value);
    }

    /** 合法 spanId：16 位小写十六进制，且不能全 0。 */
    public static boolean isValidSpanId(String value) {
        return isLowerHex(value, SPAN_ID_HEX_LENGTH) && !"0".repeat(SPAN_ID_HEX_LENGTH).equals(value);
    }

    private static boolean isLowerHex(String value, int length) {
        if (value == null || value.length() != length) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            char c = value.charAt(i);
            if ((c < '0' || c > '9') && (c < 'a' || c > 'f')) {
                return false;
            }
        }
        return true;
    }

    private static String randomHex(int length) {
        byte[] bytes = new byte[length / 2];
        RANDOM.nextBytes(bytes);
        StringBuilder sb = new StringBuilder(length);
        for (byte b : bytes) {
            sb.append(HEX.charAt((b >> 4) & 0xF)).append(HEX.charAt(b & 0xF));
        }
        // 极小概率全 0，兜底重来，保证非全 0。
        return sb.toString().matches("0+") ? randomHex(length) : sb.toString();
    }

    /** 生成合法 traceId。 */
    public static String generateTraceId() {
        return randomHex(TRACE_ID_HEX_LENGTH);
    }

    /** 生成合法 spanId。 */
    public static String generateSpanId() {
        return randomHex(SPAN_ID_HEX_LENGTH);
    }

    /**
     * 解析入站 traceId：合法则原样使用；缺失（null/空白）或非法（含大小写不合规）则生成新的。
     *
     * @return 解析后的上下文（每次都生成新的 spanId）与是否为自行生成
     */
    public static TraceResolution resolve(String incomingTraceId) {
        if (incomingTraceId != null && !incomingTraceId.isBlank()
                && isValidTraceId(incomingTraceId.trim())) {
            String traceId = incomingTraceId.trim();
            TraceContext ctx = new TraceContext(traceId, generateSpanId(), true);
            return new TraceResolution(ctx, false,
                    "入站 X-Trace-Id 合法，透传 traceId=" + traceId + "，生成新 spanId=" + ctx.spanId());
        }
        String reason;
        if (incomingTraceId == null || incomingTraceId.isBlank()) {
            reason = "入站 X-Trace-Id 缺失，自行生成合法 traceId";
        } else {
            reason = "入站 X-Trace-Id 格式非法（'" + incomingTraceId
                    + "'，要求 32 位小写十六进制且非全 0），自行生成合法 traceId";
        }
        TraceContext ctx = new TraceContext(generateTraceId(), generateSpanId(), true);
        return new TraceResolution(ctx, true, reason + "=" + ctx.traceId());
    }

    /** 解析结果。 */
    public record TraceResolution(TraceContext context, boolean generated, String reason) {
    }
}
