package com.github.lisama569.runtimetelemetryalertingjava.tests;

import com.github.lisama569.runtimetelemetryalertingjava.alert.AlertStateMachine;
import com.github.lisama569.runtimetelemetryalertingjava.model.AlertEvent;
import com.github.lisama569.runtimetelemetryalertingjava.model.AlertRule;
import com.github.lisama569.runtimetelemetryalertingjava.model.AlertState;
import com.github.lisama569.runtimetelemetryalertingjava.model.ComparisonOperator;
import com.github.lisama569.runtimetelemetryalertingjava.model.MetricPoint;
import com.github.lisama569.runtimetelemetryalertingjava.model.WindowAggregation;
import com.github.lisama569.runtimetelemetryalertingjava.model.WindowEmit;
import com.github.lisama569.runtimetelemetryalertingjava.model.WindowResultType;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 告警确认触发 / 抖动抑制 / 恢复确认 / 窗口去重 测试。
 * 规则：latency 均值 > 100，连续 3 个窗口越限才触发，连续 2 个窗口恢复才恢复。
 */
class AlertStateMachineTest {

    private static final Logger log = LoggerFactory.getLogger(AlertStateMachineTest.class);
    private static final long W = 1000;

    private AlertStateMachine machineWithEvents(List<AlertEvent> sink) {
        AlertRule rule = new AlertRule("high-latency", "latency",
                ComparisonOperator.GT, 100.0, 3, 2);
        AlertStateMachine m = new AlertStateMachine(rule);
        m.addListener(sink::add);
        return m;
    }

    /** 造一个窗口定稿结果：窗口 [start, start+W)，内含给定采样值。 */
    private WindowEmit onTime(long start, double... values) {
        WindowAggregation agg = new WindowAggregation(start, start + W, "latency");
        for (int i = 0; i < values.length; i++) {
            agg.add(new MetricPoint("latency", start + i, values[i], "id-" + start + "-" + i));
        }
        return new WindowEmit(WindowResultType.ON_TIME, agg, start + W, "test-on-time");
    }

    private AlertEvent feed(AlertStateMachine m, long start, double... values) {
        WindowEmit emit = onTime(start, values);
        AlertEvent e = m.evaluate(emit);
        log.info("[alert-test] feed windowStart={} values={} mean={} -> event={}, state={}",
                start, values, emit.aggregation().mean(), e == null ? "none" : e.toState(), m.state());
        return e;
    }

    @Test
    void firesOnlyAfterConsecutiveConfirmations() {
        List<AlertEvent> sink = new ArrayList<>();
        AlertStateMachine m = machineWithEvents(sink);

        AlertEvent e1 = feed(m, 0, 110);
        assertNotNull(e1, "判定依据: 第 1 个越限窗口产生 OK->PENDING 的可追溯迁移");
        assertEquals(AlertState.PENDING, e1.toState());
        assertEquals(AlertEvent.Kind.STATE_CHANGE, e1.kind(), "判定依据: 进入 PENDING 不是真正触发");
        assertNull(feed(m, 1000, 120), "判定依据: 第 2 个越限窗口仍在确认，无状态迁移，更不触发");
        assertEquals(AlertState.PENDING, m.state());
        AlertEvent fired = feed(m, 2000, 130);
        assertNotNull(fired, "判定依据: 连续第 3 个越限窗口必须触发一次");
        assertEquals(AlertState.FIRING, fired.toState());
        assertEquals(AlertState.PENDING, fired.fromState(), "判定依据: 触发事件必须来自 PENDING(经确认)->FIRING 的转换记录");
        assertEquals(AlertEvent.Kind.FIRED, fired.kind(), "判定依据: 该事件必须标记为真正触发 FIRED");
        assertEquals(1, sink.stream().filter(AlertEvent::triggered).count(),
                "判定依据: 到目前为止只允许真正触发一次，不能重复");
        // 已 FIRING 后继续越限：保持 FIRING，不重复触发
        assertNull(feed(m, 3000, 200), "判定依据: FIRING 中持续越限不得重复触发");
        assertEquals(AlertState.FIRING, m.state());
        assertEquals(1, sink.stream().filter(AlertEvent::triggered).count(),
                "判定依据: 持续越限不能产生第二次触发事件");
    }

    @Test
    void jitterDoesNotReTrigger() {
        List<AlertEvent> sink = new ArrayList<>();
        AlertStateMachine m = machineWithEvents(sink);

        // 2 个越限 + 1 个正常抖动 -> 确认被打断，退回 OK，从未触发
        feed(m, 0, 110);
        feed(m, 1000, 110);
        AlertEvent droppedBack = feed(m, 2000, 90);
        assertNotNull(droppedBack, "判定依据: 确认期被正常窗口打断，应有 PENDING->OK 转换可追溯");
        assertEquals(AlertState.OK, droppedBack.toState());
        assertEquals(AlertState.PENDING, droppedBack.fromState());
        assertEquals(0, sink.stream().filter(AlertEvent::triggered).count(),
                "判定依据: 抖动必须被吞掉，整个过程零触发");

        // 再来 3 个连续越限，确认计数必须从头计算（之前 2 次被作废）
        AlertEvent rePending = feed(m, 3000, 110);
        assertNotNull(rePending, "判定依据: 抖动后计数清零，第 1 次越限重新迁移到 PENDING");
        assertEquals(AlertState.PENDING, rePending.toState());
        assertEquals(AlertEvent.Kind.STATE_CHANGE, rePending.kind());
        assertNull(feed(m, 4000, 110));
        AlertEvent fired = feed(m, 5000, 110);
        assertNotNull(fired, "判定依据: 重新攒满 3 个连续越限窗口才允许触发");
        assertEquals(1, sink.stream().filter(AlertEvent::triggered).count(), "判定依据: 全程恰好触发一次");
    }

    @Test
    void recoversOnlyAfterConfirmations() {
        List<AlertEvent> sink = new ArrayList<>();
        AlertStateMachine m = machineWithEvents(sink);
        feed(m, 0, 110);
        feed(m, 1000, 110);
        assertNotNull(feed(m, 2000, 110), "前置: 3 连越限已触发");

        AlertEvent recovering = feed(m, 3000, 50);
        assertNotNull(recovering, "判定依据: FIRING 后首个正常窗口应进入 RECOVERING，而非立刻恢复");
        assertEquals(AlertState.RECOVERING, recovering.toState());

        // 恢复期再次越限 -> 否决恢复，直接回 FIRING，不产生新的 TRIGGERED 事件
        AlertEvent rejected = feed(m, 4000, 300);
        assertNotNull(rejected, "判定依据: 恢复确认中再次越限必须有可追溯的回退事件");
        assertEquals(AlertState.FIRING, rejected.toState());
        assertEquals(AlertState.RECOVERING, rejected.fromState());
        assertEquals(1, sink.stream().filter(AlertEvent::triggered).count(),
                "判定依据: 回退到 FIRING 不算二次触发，触发事件仍只有 1 个");
        assertEquals(0, sink.stream().filter(AlertEvent::recovered).count(), "判定依据: 此时绝不能恢复");

        // 再连续 2 个正常窗口才恢复
        assertNotNull(feed(m, 5000, 50), "判定依据: 重新进入 RECOVERING");
        AlertEvent recovered = feed(m, 6000, 50);
        assertNotNull(recovered, "判定依据: 连续第 2 个正常窗口必须恢复 OK");
        assertEquals(AlertState.OK, recovered.toState());
        assertEquals(1, sink.stream().filter(AlertEvent::recovered).count(), "判定依据: 全程恰好恢复一次");
    }

    @Test
    void sameWindowIsEvaluatedOnce() {
        List<AlertEvent> sink = new ArrayList<>();
        AlertStateMachine m = machineWithEvents(sink);

        WindowEmit w1 = onTime(0, 110);
        assertNotNull(m.evaluate(w1), "判定依据: 窗口首次评估进入 PENDING");
        // 同一 windowStart 的结果被重复投递（例如监听器重复注册/重放）
        assertNull(m.evaluate(w1), "判定依据: 同一窗口重复投递必须幂等忽略，不得重复累计确认次数");
        assertEquals(1, m.breachStreak(), "判定依据: 确认计数不能被同一窗口刷大");

        // 非 ON_TIME（迟到修正）不能驱动状态机
        WindowAggregation agg = new WindowAggregation(1000, 2000, "latency");
        agg.add(new MetricPoint("latency", 1001, 999, "x"));
        WindowEmit late = new WindowEmit(WindowResultType.LATE_CORRECTION, agg, 2500, "late");
        assertNull(m.evaluate(late), "判定依据: 迟到修正不能触发告警，避免迟到数据让告警抖动");
        // 别的指标不驱动本机
        WindowAggregation other = new WindowAggregation(1000, 2000, "error-rate");
        other.add(new MetricPoint("error-rate", 1001, 1.0, "y"));
        assertNull(m.evaluate(new WindowEmit(WindowResultType.ON_TIME, other, 2000, "other")),
                "判定依据: 指标名不匹配的窗口必须忽略");
        assertEquals(AlertState.PENDING, m.state(), "判定依据: 被忽略的输入不应改变状态");
    }
}
