package com.github.lisama569.runtimetelemetryalertingjava.alert;

import com.github.lisama569.runtimetelemetryalertingjava.model.AlertEvent;
import com.github.lisama569.runtimetelemetryalertingjava.model.AlertRule;
import com.github.lisama569.runtimetelemetryalertingjava.model.AlertState;
import com.github.lisama569.runtimetelemetryalertingjava.model.WindowResult;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * 单条规则的告警状态机（所有方法 synchronized，保证并发上报下状态转换一致）。
 *
 * <pre>
 * NORMAL ──越限──── PENDING ──连续越限达标──▶ FIRING（触发事件）
 *   ▲                   │ 恢复                ▲
 *   │                   ▼                     │ 越限
 *   └─恢复达标── RESOLVING ◀──恢复── FIRING ───┘
 * </pre>
 * <ul>
 *   <li>抖动：PENDING/RESOLVING 期间任一个窗口不满足，计数清零回退，
 *       状态没到 FIRING/NORMAL 就不会产生触发/恢复事件。</li>
 *   <li>每个封存窗口按 windowEnd 幂等评估一次，重复评估直接返回 null，
 *       因此既不会重复触发也不会漏触发。</li>
 * </ul>
 */
public final class AlertStateMachine {

    private final AlertRule rule;
    private final Set<Long> evaluatedWindowEnds = new HashSet<>();

    private AlertState state = AlertState.NORMAL;
    private int consecutiveBreaches;
    private int consecutiveRecoveries;

    public AlertStateMachine(AlertRule rule) {
        this.rule = Objects.requireNonNull(rule, "rule");
        if (rule.breachConfirmations() < 1 || rule.recoveryConfirmations() < 1) {
            throw new IllegalArgumentException("确认窗口数必须 >= 1");
        }
    }

    public AlertRule getRule() {
        return rule;
    }

    /**
     * 用一个封存窗口评估一次；空窗口（count=0，无样本）跳过，不改变状态。
     *
     * @return 本次状态跳变事件；无跳变或重复评估返回 null
     */
    public synchronized AlertEvent evaluate(WindowResult window) {
        Objects.requireNonNull(window, "window");
        if (!evaluatedWindowEnds.add(window.windowEnd())) {
            // 同一窗口只评估一次：重复评估不会重复触发。
            return null;
        }
        if (window.count() == 0) {
            return null;
        }

        boolean breach = rule.isBreach(window);
        double value = rule.metricValue(window);
        AlertState from = state;
        AlertState to = from;
        String reason;

        switch (from) {
            case NORMAL -> {
                if (breach) {
                    consecutiveBreaches = 1;
                    consecutiveRecoveries = 0;
                    if (consecutiveBreaches >= rule.breachConfirmations()) {
                        to = AlertState.FIRING;
                    } else {
                        to = AlertState.PENDING;
                    }
                    reason = String.format(
                            "窗口 [%d,%d) %s=%.4f > 阈值 %.4f，连续越限 %d/%d（NORMAL→%s）",
                            window.windowStart(), window.windowEnd(),
                            rule.metricKind(), value, rule.threshold(),
                            consecutiveBreaches, rule.breachConfirmations(), to);
                } else {
                    consecutiveBreaches = 0;
                    reason = String.format(
                            "窗口 [%d,%d) %s=%.4f <= 阈值 %.4f，保持 NORMAL",
                            window.windowStart(), window.windowEnd(),
                            rule.metricKind(), value, rule.threshold());
                }
            }
            case PENDING -> {
                if (breach) {
                    consecutiveBreaches++;
                    if (consecutiveBreaches >= rule.breachConfirmations()) {
                        to = AlertState.FIRING;
                    }
                    reason = String.format(
                            "窗口 [%d,%d) %s=%.4f > 阈值 %.4f，连续越限 %d/%d（PENDING→%s）",
                            window.windowStart(), window.windowEnd(),
                            rule.metricKind(), value, rule.threshold(),
                            consecutiveBreaches, rule.breachConfirmations(), to);
                } else {
                    consecutiveBreaches = 0;
                    to = AlertState.NORMAL;
                    reason = String.format(
                            "窗口 [%d,%d) %s=%.4f <= 阈值 %.4f，越限未连续达标即恢复，PENDING→NORMAL（抖动不触发）",
                            window.windowStart(), window.windowEnd(),
                            rule.metricKind(), value, rule.threshold());
                }
            }
            case FIRING -> {
                if (!breach) {
                    consecutiveRecoveries = 1;
                    consecutiveBreaches = 0;
                    if (consecutiveRecoveries >= rule.recoveryConfirmations()) {
                        to = AlertState.NORMAL;
                    } else {
                        to = AlertState.RESOLVING;
                    }
                    reason = String.format(
                            "窗口 [%d,%d) %s=%.4f <= 阈值 %.4f，连续恢复 %d/%d（FIRING→%s）",
                            window.windowStart(), window.windowEnd(),
                            rule.metricKind(), value, rule.threshold(),
                            consecutiveRecoveries, rule.recoveryConfirmations(), to);
                } else {
                    consecutiveRecoveries = 0;
                    reason = String.format(
                            "窗口 [%d,%d) %s=%.4f 仍 > 阈值 %.4f，保持 FIRING（不重复触发）",
                            window.windowStart(), window.windowEnd(),
                            rule.metricKind(), value, rule.threshold());
                }
            }
            case RESOLVING -> {
                if (!breach) {
                    consecutiveRecoveries++;
                    if (consecutiveRecoveries >= rule.recoveryConfirmations()) {
                        to = AlertState.NORMAL;
                    }
                    reason = String.format(
                            "窗口 [%d,%d) %s=%.4f <= 阈值 %.4f，连续恢复 %d/%d（RESOLVING→%s）",
                            window.windowStart(), window.windowEnd(),
                            rule.metricKind(), value, rule.threshold(),
                            consecutiveRecoveries, rule.recoveryConfirmations(), to);
                } else {
                    consecutiveRecoveries = 0;
                    to = AlertState.FIRING;
                    reason = String.format(
                            "窗口 [%d,%d) %s=%.4f 又越限 > %.4f，恢复未连续达标，RESOLVING→FIRING（抖动不产生恢复事件）",
                            window.windowStart(), window.windowEnd(),
                            rule.metricKind(), value, rule.threshold());
                }
            }
            default -> throw new IllegalStateException("未知状态: " + from);
        }

        state = to;
        // 对外事件只有两类：
        // 1) 首次真正触发：从 PENDING（或 breachConfirmations=1 时从 NORMAL）进入 FIRING；
        //    RESOLVING->FIRING 只是“恢复确认失败的抖动回退”，告警本就未恢复，不算新触发。
        // 2) 真正恢复：进入 NORMAL。
        // 进入/退出 PENDING、RESOLVING 的确认中变化均为内部状态，不外发（抖动不外发）。
        boolean isFire = to == AlertState.FIRING
                && (from == AlertState.PENDING || from == AlertState.NORMAL);
        boolean isRecover = to == AlertState.NORMAL
                && (from == AlertState.FIRING || from == AlertState.RESOLVING);
        if (to != from && (isFire || isRecover)) {
            return new AlertEvent(rule.name(), from, to, window.windowEnd(), window, reason);
        }
        return null;
    }

    public synchronized AlertState currentState() {
        return state;
    }

    public synchronized int getConsecutiveBreaches() {
        return consecutiveBreaches;
    }

    public synchronized int getConsecutiveRecoveries() {
        return consecutiveRecoveries;
    }
}
