/*
 * Copyright 1999-2019 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.csp.sentinel.slots.block.degrade.circuitbreaker;

import java.util.concurrent.atomic.AtomicReference;

import com.alibaba.csp.sentinel.Entry;
import com.alibaba.csp.sentinel.context.Context;
import com.alibaba.csp.sentinel.slotchain.ResourceWrapper;
import com.alibaba.csp.sentinel.slots.block.degrade.DegradeRule;
import com.alibaba.csp.sentinel.slots.block.degrade.DegradeRuleManager;
import com.alibaba.csp.sentinel.util.AssertUtil;
import com.alibaba.csp.sentinel.util.TimeUtil;
import com.alibaba.csp.sentinel.util.function.BiConsumer;

/**
 * @author Eric Zhao
 * @since 1.8.0
 */
public abstract class AbstractCircuitBreaker implements CircuitBreaker {
    // 熔断规则
    protected final DegradeRule rule;
    // 熔断的持续时间，对应熔断规则的timeWindow属性，只不过这里的单位是毫秒
    protected final int recoveryTimeoutMs;

    // 用于注册熔断器状态变更监听的注册器
    private final EventObserverRegistry observerRegistry;

    // 记录当前熔断器的状态
    protected final AtomicReference<State> currentState = new AtomicReference<>(State.CLOSED);

    // 允许熔断器关闭的时间，等于熔断开始时间加上熔断期间的时长
    protected volatile long nextRetryTimestamp;

    public AbstractCircuitBreaker(DegradeRule rule) {
        this(rule, EventObserverRegistry.getInstance());
    }

    AbstractCircuitBreaker(DegradeRule rule, EventObserverRegistry observerRegistry) {
        AssertUtil.notNull(observerRegistry, "observerRegistry cannot be null");
        if (!DegradeRuleManager.isValidRule(rule)) {
            throw new IllegalArgumentException("Invalid DegradeRule: " + rule);
        }
        this.observerRegistry = observerRegistry;
        this.rule = rule;
        this.recoveryTimeoutMs = rule.getTimeWindow() * 1000; // 把秒转为成毫秒
    }

    @Override
    public DegradeRule getRule() {
        return rule;
    }

    @Override
    public State currentState() {
        return currentState.get();
    }

    // SNIPPET_START tryPass
    @Override
    public boolean tryPass(Context context) {
        // Template implementation.
        // 当前熔断器处于关闭状态，则放行
        if (currentState.get() == State.CLOSED) {
            return true;
        }
        if (currentState.get() == State.OPEN) { // 开启状态
            // For half-open state we allow a request for probing.
            /*
             * 判断当前是否已经可以关闭熔断器了，如果可以则尝试从开启状态转换到关闭状态，
             * 成功推动这次转换的请求是可以通过的，但是如果该请求还是发生错误，则又会改为全开状态。
             */
            return retryTimeoutArrived() && fromOpenToHalfOpen(context);
        }
        return false; // 半开启状态，则直接拒绝请求
    }
    // SNIPPET_END tryPass

    /**
     * Reset the statistic data.
     */
    abstract void resetStat();

    // SNIPPET_START retryTimeoutArrived
    protected boolean retryTimeoutArrived() {
        // 判断当前时间是否超过了熔断器允许被关闭的时间，主要用来判断是否可以关闭熔断器了
        return TimeUtil.currentTimeMillis() >= nextRetryTimestamp;
    }
    // SNIPPET_END retryTimeoutArrived

    protected void updateNextRetryTimestamp() {
        // 最新的熔断关闭的时间等于当前时间加上熔断期的时长
        this.nextRetryTimestamp = TimeUtil.currentTimeMillis() + recoveryTimeoutMs;
    }

    // SNIPPET_START fromCloseToOpen
    protected boolean fromCloseToOpen(double snapshotValue) {
        State prev = State.CLOSED;
        if (currentState.compareAndSet(prev, State.OPEN)) {
            // 更新熔断器下一次关闭的时间
            updateNextRetryTimestamp();

            notifyObservers(prev, State.OPEN, snapshotValue);
            return true;
        }
        return false;
    }
    // SNIPPET_END fromCloseToOpen

    // SNIPPET_START fromOpenToHalfOpen
    protected boolean fromOpenToHalfOpen(Context context) {
        // 将状态修改为半开状态
        if (currentState.compareAndSet(State.OPEN, State.HALF_OPEN)) {
            // 通知观察者
            notifyObservers(State.OPEN, State.HALF_OPEN, null);
            // 获取资源入口
            Entry entry = context.getCurEntry();
            /*
             * 向entry注册exit回调，之所以要注册回调，是修复1638号issue提出的BUG。
             * 在当前请求结束后执行下面的accept回调函数
             */
            entry.whenTerminate(new BiConsumer<Context, Entry>() {
                @Override
                public void accept(Context context, Entry entry) {
                    // Note: This works as a temporary workaround for https://github.com/alibaba/Sentinel/issues/1638
                    // Without the hook, the circuit breaker won't recover from half-open state in some circumstances
                    // when the request is actually blocked by upcoming rules (not only degrade rules).
                    // 如果改为半开后发生了错误，
                    if (entry.getBlockError() != null) {
                        // Fallback to OPEN due to detecting request is blocked
                        // 则再次从半开状态修改为全开状态
                        currentState.compareAndSet(State.HALF_OPEN, State.OPEN);
                        notifyObservers(State.HALF_OPEN, State.OPEN, 1.0d);
                    }
                }
            });
            return true;
        }
        return false;
    }
    // SNIPPET_END fromOpenToHalfOpen
    
    private void notifyObservers(CircuitBreaker.State prevState, CircuitBreaker.State newState, Double snapshotValue) {
        for (CircuitBreakerStateChangeObserver observer : observerRegistry.getStateChangeObservers()) {
            observer.onStateChange(prevState, newState, rule, snapshotValue);
        }
    }

    // SNIPPET_START fromHalfOpenToOpen
    protected boolean fromHalfOpenToOpen(double snapshotValue) {
        // 从半开状态改为全开
        if (currentState.compareAndSet(State.HALF_OPEN, State.OPEN)) {
            // 更新熔断器下一次关闭的时间
            updateNextRetryTimestamp();
            notifyObservers(State.HALF_OPEN, State.OPEN, snapshotValue);
            return true;
        }
        return false;
    }
    // SNIPPET_END fromHalfOpenToOpen

    // SNIPPET_START fromHalfOpenToClose
    protected boolean fromHalfOpenToClose() {
        if (currentState.compareAndSet(State.HALF_OPEN, State.CLOSED)) {
            // 重置数据统计
            resetStat();
            // 通知观察者
            notifyObservers(State.HALF_OPEN, State.CLOSED, null);
            return true;
        }
        return false;
    }
    // SNIPPET_END fromHalfOpenToClose

    // SNIPPET_START transformToOpen
    protected void transformToOpen(double triggerValue) {
        State cs = currentState.get();
        switch (cs) {
            case CLOSED:
                fromCloseToOpen(triggerValue);
                break;
            case HALF_OPEN:
                fromHalfOpenToOpen(triggerValue);
                break;
            default:
                break;
        }
    }
    // SNIPPET_END transformToOpen
}
