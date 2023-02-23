package com.lingh.hint;

import groovy.lang.Closure;
import groovy.util.Expando;
import org.apache.shardingsphere.infra.util.exception.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.util.expr.InlineExpressionParser;
import org.apache.shardingsphere.sharding.api.sharding.hint.HintShardingAlgorithm;
import org.apache.shardingsphere.sharding.api.sharding.hint.HintShardingValue;
import org.apache.shardingsphere.sharding.exception.algorithm.sharding.ShardingAlgorithmInitializationException;

import java.util.Collection;
import java.util.Properties;
import java.util.stream.Collectors;

public final class HintInlineJavascriptShardingAlgorithm implements HintShardingAlgorithm<Comparable<?>> {

    private static final String ALGORITHM_EXPRESSION_KEY = "algorithm-expression";

    private static final String DEFAULT_ALGORITHM_EXPRESSION = "${value}";

    private static final String HINT_INLINE_VALUE_PROPERTY_NAME = "value";

    private String algorithmExpression;

    @Override
    public void init(final Properties props) {
        String algorithmExpression = props.getProperty(ALGORITHM_EXPRESSION_KEY, DEFAULT_ALGORITHM_EXPRESSION);
        ShardingSpherePreconditions.checkNotNull(algorithmExpression, () -> new ShardingAlgorithmInitializationException(getType(), "Inline sharding algorithm expression can not be null."));
        this.algorithmExpression = InlineExpressionParser.handlePlaceHolder(algorithmExpression.trim());
    }

    @Override
    public Collection<String> doSharding(final Collection<String> availableTargetNames, final HintShardingValue<Comparable<?>> shardingValue) {
        return shardingValue.getValues().isEmpty() ? availableTargetNames : shardingValue.getValues().stream().map(shardingValue1 -> {
            Closure<?> closure = new InlineExpressionParser(algorithmExpression).evaluateClosure().rehydrate(new Expando(), null, null);
            closure.setResolveStrategy(Closure.DELEGATE_ONLY);
            closure.setProperty(HINT_INLINE_VALUE_PROPERTY_NAME, shardingValue1);
            return closure.call().toString();
        }).collect(Collectors.toList());
    }

    @Override
    public String getType() {
        return "HINT_INLINE";
    }
}
