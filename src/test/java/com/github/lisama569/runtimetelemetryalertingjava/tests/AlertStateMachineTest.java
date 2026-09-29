package com.github.lisama569.runtimetelemetryalertingjava.tests;

import com.github.lisama569.runtimetelemetryalertingjava.alert.AlertEngine;
import com.github.lisama569.runtimetelemetryalertingjava.alert.AlertStateMachine;
import com.github.lisama569.runtimetelemetryalertingjava.model.AlertEvent;
import com.github.lisama569.runtimetelemetryalertingjava.model.AlertRule;
import com.github.lisama569.runtimetelemetryalertingjava.model.AlertState;
import com.github.lisama569.runtimetelemetryalertingjava.model.MetricKind;
import com.github.lisama569.runtimetelemetryalertingjava.model.WindowResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 告警状态机：连续越限才触发、单窗口幂等、并发上报状态转换一致（不重不漏）。
 */
class AlertStateMachineTest {

    private static final Logger log = LoggerFactory.getLogger(AlertStateMachineTest.class);

    private AlertRule latencyRule(double threshold, int breach, int recovery) {
        return new AlertRule("latency", MetricKind.LATENCY_AVG_MS, threshold, breach, recovery);
    }

    /** count 个请求，全部耗时 latency、错误标志按 needError。 */
    private WindowResult window(long start, int count, long latency) {
        return new WindowResult(start, start + 1000, count, 0, latency * count);
    }

    private AlertEvent evaluateAndLog(AlertStateMachine m, WindowResult w) {
        AlertEvent e = m.evaluate(w);
        log.info("[输入] 窗口[{},{}) avg={} => [判定] state={} 越限连续={} 恢复连续={} 跳变={}",
                w.windowStart(), w.windowEnd(), w.averageLatencyMillis(),
                m.currentState(), m.getConsecutiveBreaches(), m.getConsecutiveRecoveries(),
                e == null ? "无" : e.from() + "->" + e.to() + " | " + e.reason());
        return e;
    }

    @Test
    @DisplayName("连续 2 窗越限才触发，单窗越限保持 PENDING")
    void firesOnlyAfterConsecutiveBreaches() {
        AlertStateMachine machine = new AlertStateMachine(latencyRule(300d, 2, 2));

        assertNull(evaluateAndLog(machine, window(0, 1, 100)), "正常窗口不产生事件");
        assertEquals(AlertState.NORMAL, machine.currentState(), "全部正常时状态为 NORMAL");

        AlertEvent firstBreach = evaluateAndLog(machine, window(1000, 1, 500));
        assertNull(firstBreach, "第一次越限只进入 PENDING，不能立即触发");
        assertEquals(AlertState.PENDING, machine.currentState(), "单窗越限应为 PENDING");

        AlertEvent fired = evaluateAndLog(machine, window(2000, 1, 600));
        assertNotNull(fired, "连续第二个越限窗口必须产生触发事件");
        assertEquals(AlertState.PENDING, fired.from(), "触发事件来源状态应为 PENDING");
        assertEquals(AlertState.FIRING, fired.to(), "触发事件目标状态应为 FIRING");
        assertEquals(AlertState.FIRING, machine.currentState(), "触发后状态应为 FIRING");
        assertEquals("latency", fired.ruleName(), "事件必须带规则名");
    }

    @Test
    @DisplayName("同一窗口重复评估：只触发一次，不重复触发")
    void sameWindowEvaluatedIsIdempotent() {
        AlertStateMachine machine = new AlertStateMachine(latencyRule(300d, 1, 1));
        WindowResult w1 = window(0, 1, 500);

        AlertEvent first = machine.evaluate(w1);
        assertNotNull(first, "阈值=连续1窗：首个越限窗口即触发");
        assertNull(machine.evaluate(w1), "同一 windowEnd 第二次评估必须返回 null（幂等）");
        assertNull(machine.evaluate(w1), "同一 windowEnd 第三次评估仍必须返回 null");
        assertEquals(AlertState.FIRING, machine.currentState(), "重复评估不得改变状态");
    }

    @Test
    @DisplayName("并发上报：多线程同时评估，每个封存窗口恰好一次状态推进，触发不重不漏")
    void concurrentEvaluationIsConsistent() throws Exception {
        // 连续 8 个越限窗口；每个窗口被 16 个线程并发 evaluate，
        // 正确语义：每个窗口只有一个线程推动状态，其余线程拿到 null。
        AlertStateMachine machine = new AlertStateMachine(latencyRule(300d, 2, 2));
        int windows = 8;
        int threadsPerWindow = 16;

        ExecutorService pool = Executors.newFixedThreadPool(8);
        AtomicInteger transitionCount = new AtomicInteger();
        List<AlertEvent> allTransitions = java.util.Collections.synchronizedList(new ArrayList<>());
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(windows * threadsPerWindow);

        for (int w = 0; w < windows; w++) {
            WindowResult result = window(w * 1000L, 1, 500); // 每个窗口都越限
            for (int i = 0; i < threadsPerWindow; i++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        AlertEvent e = machine.evaluate(result);
                        if (e != null) {
                            transitionCount.incrementAndGet();
                            allTransitions.add(e);
                        }
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
        }
        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS), "并发任务应在时限内完成");
        pool.shutdownNow();

        log.info("[输入] {} 个越限窗口 × {} 线程并发评估 => [判定] 状态={} 实际跳变事件数={}",
                windows, threadsPerWindow, machine.currentState(), transitionCount.get());
        allTransitions.forEach(e -> log.info("[跳变] {} {}->{} @{} | {}",
                e.ruleName(), e.from(), e.to(), e.timestampMillis(), e.reason()));

        // 连续越限：第 1 窗 NORMAL->PENDING（内部，不外发），第 2 窗 PENDING->FIRING（触发1次），
        // 之后保持 FIRING。对外“触发事件”恰好 1 个。
        assertEquals(1, transitionCount.get(),
                "并发下必须恰好产生 1 个对外触发事件，不能重复触发也不能漏触发");
        assertEquals(AlertState.FIRING, machine.currentState(), "最终状态必须稳定为 FIRING");
        assertEquals(AlertState.PENDING, allTransitions.get(0).from(), "唯一触发事件应从 PENDING 出发");
        assertEquals(AlertState.FIRING, allTransitions.get(0).to(), "唯一触发事件应到 FIRING");
    }

    @Test
    @DisplayName("空窗口（无样本）不参与判定，不会误恢复")
    void emptyWindowsAreIgnored() {
        AlertStateMachine machine = new AlertStateMachine(latencyRule(300d, 1, 1));
        assertNotNull(machine.evaluate(window(0, 1, 500)), "越限触发");
        assertEquals(AlertState.FIRING, machine.currentState(), "前置：已 FIRING");

        WindowResult empty = new WindowResult(1000, 2000, 0, 0, 0);
        assertNull(machine.evaluate(empty), "空窗口必须被跳过，不能当作“恢复”");
        assertEquals(AlertState.FIRING, machine.currentState(), "空窗口不能改变 FIRING 状态");
    }

    @Test
    @DisplayName("多规则引擎：各状态机独立计数、独立触发")
    void engineEvaluatesRulesIndependently() {
        AlertRule highLatency = latencyRule(300d, 1, 1);
        AlertRule errorRule = new AlertRule("errors", MetricKind.ERROR_RATE, 0.5d, 1, 1);
        AlertEngine engine = new AlertEngine(List.of(highLatency, errorRule));

        // 高耗时但无错误：只应触发 latency 规则
        WindowResult slowButHealthy = new WindowResult(0, 1000, 2, 0, 2000L); // avg=1000, err=0
        List<AlertEvent> events = engine.evaluate(slowButHealthy);
        events.forEach(e -> log.info("[输入] avg=1000 errorRate=0 => [判定] 跳变 {} {}->{}",
                e.ruleName(), e.from(), e.to()));

        assertEquals(1, events.size(), "只有耗时规则应触发，错误率规则不能误触发");
        assertEquals("latency", events.get(0).ruleName(), "触发的必须是 latency 规则");
        assertEquals(AlertState.FIRING, engine.stateOf("latency"), "latency 状态为 FIRING");
        assertEquals(AlertState.NORMAL, engine.stateOf("errors"), "errors 状态保持 NORMAL");
    }
}
