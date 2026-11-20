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
package com.alibaba.csp.sentinel.adapter.gateway.sc;

import com.alibaba.csp.sentinel.EntryType;
import com.alibaba.csp.sentinel.ResourceTypeConstants;
import com.alibaba.csp.sentinel.adapter.gateway.common.SentinelGatewayConstants;
import com.alibaba.csp.sentinel.adapter.gateway.common.param.GatewayParamParser;
import com.alibaba.csp.sentinel.adapter.gateway.common.param.RequestItemParser;
import com.alibaba.csp.sentinel.adapter.gateway.sc.api.GatewayApiMatcherManager;
import com.alibaba.csp.sentinel.adapter.gateway.sc.api.matcher.WebExchangeApiMatcher;
import com.alibaba.csp.sentinel.adapter.gateway.sc.callback.GatewayCallbackManager;
import com.alibaba.csp.sentinel.adapter.reactor.ContextConfig;
import com.alibaba.csp.sentinel.adapter.reactor.EntryConfig;
import com.alibaba.csp.sentinel.adapter.reactor.SentinelReactorTransformer;
import com.alibaba.csp.sentinel.util.AssertUtil;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.core.Ordered;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Eric Zhao
 * @since 1.6.0
 */
public class SentinelGatewayFilter implements GatewayFilter, GlobalFilter, Ordered {

    private final int order;

    private final GatewayParamParser<ServerWebExchange> paramParser;

    public SentinelGatewayFilter() {
        // 优先级最高
        this(Ordered.HIGHEST_PRECEDENCE);
    }

    public SentinelGatewayFilter(int order) {
        this(order, new ServerWebExchangeItemParser());
    }

    public SentinelGatewayFilter(RequestItemParser<ServerWebExchange> serverWebExchangeItemParser) {
        this(Ordered.HIGHEST_PRECEDENCE, serverWebExchangeItemParser);
    }

    public SentinelGatewayFilter(int order, RequestItemParser<ServerWebExchange> requestItemParser) {
        AssertUtil.notNull(requestItemParser, "requestItemParser cannot be null");
        this.order = order;
        this.paramParser = new GatewayParamParser<>(requestItemParser);
    }

    /*
     * 和spring-cloud-gateway整合的入口
     */
    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        // 获取路由
        Route route = exchange.getAttribute(ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR);

        // 执行其他的过滤器
        Mono<Void> asyncResult = chain.filter(exchange);
        if (route != null) {
            // 获取路由的id
            String routeId = route.getId();
            // 解析入参
            Object[] params = paramParser.parseParameterFor(routeId, exchange,
                r -> r.getResourceMode() == SentinelGatewayConstants.RESOURCE_MODE_ROUTE_ID);
            // 获取请求来源
            String origin = Optional.ofNullable(GatewayCallbackManager.getRequestOriginParser())
                .map(f -> f.apply(exchange))
                .orElse("");
            //
            asyncResult = asyncResult.transform(
                new SentinelReactorTransformer<>(new EntryConfig(routeId, ResourceTypeConstants.COMMON_API_GATEWAY,
                    EntryType.IN, 1, params, new ContextConfig(contextName(routeId), origin)))
            );
        }

        Set<String> matchingApis = pickMatchingApiDefinitions(exchange);
        for (String apiName : matchingApis) {
            // 解析入参
            Object[] params = paramParser.parseParameterFor(apiName, exchange,
                r -> r.getResourceMode() == SentinelGatewayConstants.RESOURCE_MODE_CUSTOM_API_NAME);
            //
            asyncResult = asyncResult.transform(
                new SentinelReactorTransformer<>(new EntryConfig(apiName, ResourceTypeConstants.COMMON_API_GATEWAY,
                    EntryType.IN, 1, params))
            );
        }

        return asyncResult;
    }

    private String contextName(String route) {
        return SentinelGatewayConstants.GATEWAY_CONTEXT_ROUTE_PREFIX + route;
    }

    Set<String> pickMatchingApiDefinitions(ServerWebExchange exchange) {
        return GatewayApiMatcherManager.getApiMatcherMap().values()
            .stream()
            .filter(m -> m.test(exchange)) // 过滤当前请求能够匹配的API
            .map(WebExchangeApiMatcher::getApiName)
            .collect(Collectors.toSet());
    }

    @Override
    public int getOrder() {
        return order;
    }
}
