package com.github.lisama569.runtimetelemetryalertingjava.trace;

import java.security.SecureRandom;
import java.util.regex.Pattern;

/**
 * TraceId / SpanId 的校验与生成。
 *
 * 规则：
 * - traceId：16~32 位十六进制字符，且不能全 0。
 * - spanId：16 位十六进制字符，且不能全 0。
 * - 入站标识缺失或不合法时，由本类生成一个合法 traceId，保证链路不断。
 */
public final class TraceIds {

    public static final Pattern TRACE_ID_PATTERN = Pattern.compile("[0-9a-fA-F]{16,32}");
    public static final Pattern SPAN_ID_PATTERN = Pattern.compile("[0-9a-fA-F]{16}");

    private static final SecureRandom RANDOM = new SecureRandom();

    private TraceIds() {
    }

    public static boolean isValidTraceId(String traceId) {
        return traceId != null
                && TRACE_ID_PATTERN.matcher(traceId).matches()
                && !isAllZero(traceId);
    }

    public static boolean isValidSpanId(String spanId) {
        return spanId != null
                && SPAN_ID_PATTERN.matcher(spanId).matches()
                && !isAllZero(spanId);
    }

    public static boolean isAllZero(String id) {
        if (id == null || id.isEmpty()) {
            return true;
        }
        for (int i = 0; i < id.length(); i++) {
            if (id.charAt(i) != '0') {
                return false;
            }
        }
        return true;
    }

    /** 生成 32 位十六进制 traceId。 */
    public static String generateTraceId() {
        return randomHex(32);
    }

    /** 生成 16 位十六进制 spanId。 */
    public static String generateSpanId() {
        return randomHex(16);
    }

    /**
     * 解析入站 traceId：合法则规范化为小写并透传；
     * null/空/格式非法/全 0 时新生成一个。
     */
    public static String resolveInboundTraceId(String inbound) {
        if (isValidTraceId(inbound)) {
            return inbound.toLowerCase();
        }
        String generated = generateTraceId();
        org.slf4j.LoggerFactory.getLogger(TraceIds.class)
                .info("[trace] inbound traceId '{}' missing or invalid, generated new traceId={}",
                        inbound, generated);
        return generated;
    }

    private static String randomHex(int length) {
        StringBuilder sb = new StringBuilder(length);
        while (sb.length() < length) {
            sb.append(Long.toHexString(RANDOM.nextLong()));
        }
        return sb.substring(0, length);
    }
}
