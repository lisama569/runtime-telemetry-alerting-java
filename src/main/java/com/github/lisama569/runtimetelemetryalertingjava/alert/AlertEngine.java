package com.github.lisama569.runtimetelemetryalertingjava.alert;

import com.github.lisama569.runtimetelemetryalertingjava.model.AlertEvent;
import com.github.lisama569.runtimetelemetryalertingjava.model.AlertRule;
import com.github.lisama569.runtimetelemetryalertingjava.model.AlertState;
import com.github.lisama569.runtimetelemetryalertingjava.model.WindowResult;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 多条规则的告警引擎：一个封存窗口分发给每条规则的状态机评估，
 * 收集状态跳变事件。各状态机内部已同步，整链路由 pipeline 再串行封存。
 */
public final class AlertEngine {

    private final Map<String, AlertStateMachine> machines = new LinkedHashMap<>();

    public AlertEngine(List<AlertRule> rules) {
        for (AlertRule rule : rules) {
            if (machines.put(rule.name(), new AlertStateMachine(rule)) != null) {
                throw new IllegalArgumentException("规则名重复: " + rule.name());
            }
        }
    }

    public List<AlertEvent> evaluate(WindowResult window) {
        List<AlertEvent> events = new ArrayList<>();
        for (AlertStateMachine machine : machines.values()) {
            AlertEvent event = machine.evaluate(window);
            if (event != null) {
                events.add(event);
            }
        }
        return events;
    }

    public AlertState stateOf(String ruleName) {
        AlertStateMachine machine = machines.get(ruleName);
        return machine == null ? null : machine.currentState();
    }

    public List<AlertRule> getRules() {
        List<AlertRule> rules = new ArrayList<>();
        for (AlertStateMachine machine : machines.values()) {
            rules.add(machine.getRule());
        }
        return rules;
    }
}
