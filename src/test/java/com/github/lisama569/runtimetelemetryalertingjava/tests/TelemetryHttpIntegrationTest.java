package com.github.lisama569.runtimetelemetryalertingjava.tests;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * HTTP 端到端：入站 X-Trace-Id 解析/生成、指标上报、watermark 推进、告警查询。
 * 用固定时间戳（毫秒），不依赖墙上时钟。
 */
@SpringBootTest
@AutoConfigureMockMvc
class TelemetryHttpIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    @DisplayName("合法 X-Trace-Id 原样回显并透传到当前链路")
    void validTraceIdIsEchoed() throws Exception {
        String valid = "4bf92f3577b34da6a3ce929d0e0e4736";
        mockMvc.perform(get("/telemetry/trace").header("X-Trace-Id", valid))
                .andExpect(status().isOk())
                .andExpect(header().string("X-Trace-Id", valid))
                .andExpect(jsonPath("$.traceId").value(valid))
                .andExpect(jsonPath("$.spanId").isNotEmpty());
    }

    @Test
    @DisplayName("缺失 X-Trace-Id 时服务端生成合法 ID 并回写响应头")
    void missingTraceIdIsGenerated() throws Exception {
        mockMvc.perform(get("/telemetry/trace"))
                .andExpect(status().isOk())
                .andExpect(header().string("X-Trace-Id",
                        org.hamcrest.Matchers.matchesPattern("[0-9a-f]{32}")))
                .andExpect(jsonPath("$.traceId",
                        org.hamcrest.Matchers.matchesPattern("[0-9a-f]{32}")));
    }

    @Test
    @DisplayName("完整链路：乱序/重复上报 -> 推进封存 -> 告警触发结果可通过 HTTP 查询")
    void reportAdvanceAndAlertOverHttp() throws Exception {
        // 窗口 [0,1000)：3 条慢请求（avg=500 > 300）；其中一条重复上报
        long[] times = {900L, 100L, 500L, 500L};
        String[] ids = {"http-1", "http-2", "http-3", "http-3"}; // 最后一条重复
        for (int i = 0; i < times.length; i++) {
            String body = "{\"eventId\":\"" + ids[i] + "\",\"eventTime\":" + times[i]
                    + ",\"latencyMillis\":500,\"error\":false}";
            String expectedOutcome = i == 3 ? "DUPLICATE" : "ACCEPTED";
            mockMvc.perform(post("/telemetry/events")
                            .contentType(MediaType.APPLICATION_JSON).content(body))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.outcome").value(expectedOutcome))
                    .andExpect(jsonPath("$.reason").isNotEmpty());
        }

        // watermark=1500 => horizon=1000，封存 [0,1000)
        mockMvc.perform(post("/telemetry/advance")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"watermark\":1500}"))
                .andExpect(status().isOk());

        // 再上报并封存第二个越限窗口 [1000,2000)，凑满连续 2 窗触发
        for (int i = 0; i < 2; i++) {
            String body = "{\"eventId\":\"http-w2-" + i + "\",\"eventTime\":"
                    + (1100L + i * 100) + ",\"latencyMillis\":500,\"error\":false}";
            mockMvc.perform(post("/telemetry/events")
                            .contentType(MediaType.APPLICATION_JSON).content(body))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.outcome").value("ACCEPTED"));
        }
        mockMvc.perform(post("/telemetry/advance")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"watermark\":2500}"))
                .andExpect(status().isOk());

        mockMvc.perform(get("/telemetry/windows"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2))
                .andExpect(jsonPath("$[0].count").value(3))   // 重复没算重
                .andExpect(jsonPath("$[1].count").value(2));

        mockMvc.perform(get("/telemetry/alerts"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.latency-high").value("FIRING"))
                .andExpect(jsonPath("$.events[0].ruleName").value("latency-high"))
                .andExpect(jsonPath("$.events[0].to").value("FIRING"))
                .andExpect(jsonPath("$.events[0].reason").isNotEmpty());
    }
}
