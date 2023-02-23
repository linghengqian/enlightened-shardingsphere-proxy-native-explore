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

/**
 * Hint inline sharding algorithm.
 */
public final class HintInlineJavascriptShardingAlgorithm implements HintShardingAlgorithm<Comparable<?>> {
    
    private static final String ALGORITHM_EXPRESSION_KEY = "algorithm-expression";
    
    private static final String DEFAULT_ALGORITHM_EXPRESSION = "${value}";
    
    private static final String HINT_INLINE_VALUE_PROPERTY_NAME = "value";
    
    private String algorithmExpression;
    
    @Override
    public void init(final Properties props) {
        algorithmExpression = getAlgorithmExpression(props);
    }
    
    private String getAlgorithmExpression(final Properties props) {
        String algorithmExpression = props.getProperty(ALGORITHM_EXPRESSION_KEY, DEFAULT_ALGORITHM_EXPRESSION);
        ShardingSpherePreconditions.checkNotNull(algorithmExpression, () -> new ShardingAlgorithmInitializationException(getType(), "Inline sharding algorithm expression can not be null."));
        return InlineExpressionParser.handlePlaceHolder(algorithmExpression.trim());
    }
    
    @Override
    public Collection<String> doSharding(final Collection<String> availableTargetNames, final HintShardingValue<Comparable<?>> shardingValue) {
        return shardingValue.getValues().isEmpty() ? availableTargetNames : shardingValue.getValues().stream().map(this::doSharding).collect(Collectors.toList());
    }
    
    private String doSharding(final Comparable<?> shardingValue) {
        Closure<?> closure = createClosure();
        closure.setProperty(HINT_INLINE_VALUE_PROPERTY_NAME, shardingValue);
        return closure.call().toString();
    }
    
    private Closure<?> createClosure() {
        Closure<?> result = new InlineExpressionParser(algorithmExpression).evaluateClosure().rehydrate(new Expando(), null, null);
        result.setResolveStrategy(Closure.DELEGATE_ONLY);
        return result;
    }
    
    @Override
    public String getType() {
        return "HINT_INLINE";
    }
}
