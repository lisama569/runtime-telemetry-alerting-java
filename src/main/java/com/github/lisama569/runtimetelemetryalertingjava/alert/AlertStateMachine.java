package com.github.lisama569.runtimetelemetryalertingjava.alert;

import com.github.lisama569.runtimetelemetryalertingjava.model.AlertEvent;
import com.github.lisama569.runtimetelemetryalertingjava.model.AlertRule;
import com.github.lisama569.runtimetelemetryalertingjava.model.AlertState;
import com.github.lisama569.runtimetelemetryalertingjava.model.WindowAggregation;
import com.github.lisama569.runtimetelemetryalertingjava.model.WindowEmit;
import com.github.lisama569.runtimetelemetryalertingjava.model.WindowResultType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 单条告警规则的状态机（线程安全）。
 *
 * 四态语义：
 * - OK：正常。连续越限窗口数达到 triggerConfirmations 才离开。
 * - PENDING：越限确认中。中途出现一个未越限窗口即退回 OK（抖动被吞掉，不触发）。
 * - FIRING：已触发告警。出现未越限窗口后进入 RECOVERING。
 * - RECOVERING：恢复确认中。连续未越限窗口数达到 recoverConfirmations 才恢复 OK；
 *   中途再次越限则立即回到 FIRING（恢复被否决）。
 *
 * 只有 ON_TIME 的窗口定稿结果会驱动状态机；LATE_CORRECTION 不重复驱动，
 * 避免迟到修正造成告警抖动。同一 windowStart 只评估一次（去重，幂等）。
 * 所有转换在 synchronized 块内完成，保证并发上报下不重复触发、不漏转换。
 */
public class AlertStateMachine {

    private static final Logger log = LoggerFactory.getLogger(AlertStateMachine.class);

    private final AlertRule rule;
    private final List<AlertListener> listeners = new CopyOnWriteArrayList<>();
    private final Set<Long> seenWindowStarts = ConcurrentHashMap.newKeySet();
    private final List<AlertEvent> eventLog = new CopyOnWriteArrayList<>();

    private AlertState state = AlertState.OK;
    private int breachStreak;
    private int okStreak;

    public AlertStateMachine(AlertRule rule) {
        this.rule = rule;
    }

    public AlertRule rule() {
        return rule;
    }

    public synchronized AlertState state() {
        return state;
    }

    public synchronized int breachStreak() {
        return breachStreak;
    }

    public synchronized int okStreak() {
        return okStreak;
    }

    /** 已被本状态机评估（去重后）的窗口数量，测试/监控用。 */
    public int evaluatedWindowCount() {
        return seenWindowStarts.size();
    }

    public void addListener(AlertListener listener) {
        listeners.add(listener);
    }

    /**
     * 评估一个窗口产出。
     *
     * @return 本次产生的状态转换事件；无转换 / 非本规则指标 / 非 ON_TIME / 重复窗口时返回 null。
     */
    public AlertEvent evaluate(WindowEmit emit) {
        if (emit == null || emit.type() != WindowResultType.ON_TIME) {
            return null;
        }
        WindowAggregation agg = emit.aggregation();
        if (agg == null || !rule.metricName().equals(agg.metricName())) {
            return null;
        }
        // 同一窗口定稿只评估一次：重复投递不会造成重复触发或漏判
        if (!seenWindowStarts.add(agg.windowStart())) {
            log.info("[alert:{}] ignore duplicate evaluation of windowStart={} in state={}",
                    rule.name(), agg.windowStart(), state);
            return null;
        }

        double value = agg.observedValue();
        boolean breaching = rule.isBreaching(value);
        AlertEvent event;
        synchronized (this) {
            event = transition(agg, value, breaching);
        }
        log.info("[alert:{}] window {}~{} value={} threshold={} breaching={} -> state={} streak(breach={},ok={}) event={}",
                rule.name(), agg.windowStart(), agg.windowEnd(), value, rule.threshold(),
                breaching, state, breachStreak, okStreak, event == null ? "none" : event.toState());
        if (event != null) {
            eventLog.add(event);
            for (AlertListener l : listeners) {
                l.onAlertEvent(event);
            }
        }
        return event;
    }

    /** 在锁内完成状态转换；调用前已完成窗口去重。 */
    private AlertEvent transition(WindowAggregation agg, double value, boolean breaching) {
        long now = System.currentTimeMillis();
        return switch (state) {
            case OK -> {
                if (breaching) {
                    breachStreak = 1;
                    if (breachStreak >= rule.triggerConfirmations()) {
                        yield fire(agg, value, "breach count 1 reached triggerConfirmations="
                                + rule.triggerConfirmations(), now);
                    }
                    yield move(agg, value, AlertState.PENDING,
                            "first breaching window, confirming (1/" + rule.triggerConfirmations() + ")", now);
                }
                yield null;
            }
            case PENDING -> {
                if (breaching) {
                    breachStreak++;
                    if (breachStreak >= rule.triggerConfirmations()) {
                        yield fire(agg, value, "consecutive breaching windows " + breachStreak
                                + " reached triggerConfirmations=" + rule.triggerConfirmations(), now);
                    }
                    yield null;
                }
                // 确认期间被一个正常窗口打断：抖动吞掉，退回 OK，不触发
                breachStreak = 0;
                yield move(agg, value, AlertState.OK,
                        "breach confirmation interrupted by non-breaching window, jitter suppressed", now);
            }
            case FIRING -> {
                if (!breaching) {
                    okStreak = 1;
                    if (okStreak >= rule.recoverConfirmations()) {
                        // recoverConfirmations=1：首个正常窗口即确认恢复
                        AlertState from = state;
                        state = AlertState.OK;
                        okStreak = 0;
                        breachStreak = 0;
                        yield new AlertEvent(AlertEvent.Kind.RECOVERED, rule.name(), from, AlertState.OK, value,
                                agg.windowStart(), agg.windowEnd(),
                                "first non-breaching window reached recoverConfirmations="
                                        + rule.recoverConfirmations(), now);
                    }
                    yield move(agg, value, AlertState.RECOVERING,
                            "first non-breaching window, confirming recovery (1/"
                                    + rule.recoverConfirmations() + ")", now);
                }
                yield null;
            }
            case RECOVERING -> {
                if (breaching) {
                    // 恢复确认期间再次越限：否决恢复，立即回到 FIRING
                    okStreak = 0;
                    yield backToFiring(agg, value,
                            "breaching again during recovery confirmation, recovery rejected", now);
                }
                okStreak++;
                if (okStreak >= rule.recoverConfirmations()) {
                    AlertState from = state;
                    state = AlertState.OK;
                    okStreak = 0;
                    breachStreak = 0;
                    AlertEvent recovered = new AlertEvent(AlertEvent.Kind.RECOVERED, rule.name(), from, AlertState.OK, value,
                            agg.windowStart(), agg.windowEnd(),
                            "consecutive non-breaching windows reached recoverConfirmations="
                                    + rule.recoverConfirmations(), now);
                    log.info("[alert:{}] RECOVERED {}", rule.name(), recovered.reason());
                    yield recovered;
                }
                yield null;
            }
        };
    }

    private AlertEvent fire(WindowAggregation agg, double value, String reason, long now) {
        AlertState from = state;
        state = AlertState.FIRING;
        breachStreak = 0;
        okStreak = 0;
        AlertEvent e = new AlertEvent(AlertEvent.Kind.FIRED, rule.name(), from, AlertState.FIRING, value,
                agg.windowStart(), agg.windowEnd(), reason, now);
        log.info("[alert:{}] TRIGGERED {}", rule.name(), reason);
        return e;
    }

    private AlertEvent backToFiring(WindowAggregation agg, double value, String reason, long now) {
        AlertState from = state;
        state = AlertState.FIRING;
        return new AlertEvent(AlertEvent.Kind.STATE_CHANGE, rule.name(), from, AlertState.FIRING, value,
                agg.windowStart(), agg.windowEnd(), reason, now);
    }

    private AlertEvent move(WindowAggregation agg, double value, AlertState to, String reason, long now) {
        AlertState from = state;
        state = to;
        return new AlertEvent(AlertEvent.Kind.STATE_CHANGE, rule.name(), from, to, value,
                agg.windowStart(), agg.windowEnd(), reason, now);
    }

    public List<AlertEvent> events() {
        return List.copyOf(eventLog);
    }
}
