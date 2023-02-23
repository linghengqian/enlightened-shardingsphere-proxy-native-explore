package com.lingh.inline;

import groovy.lang.Closure;
import groovy.lang.MissingMethodException;
import groovy.util.Expando;
import org.apache.shardingsphere.infra.util.exception.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.util.exception.external.sql.type.generic.UnsupportedSQLOperationException;
import org.apache.shardingsphere.infra.util.expr.InlineExpressionParser;
import org.apache.shardingsphere.sharding.api.sharding.standard.PreciseShardingValue;
import org.apache.shardingsphere.sharding.api.sharding.standard.RangeShardingValue;
import org.apache.shardingsphere.sharding.api.sharding.standard.StandardShardingAlgorithm;
import org.apache.shardingsphere.sharding.exception.algorithm.sharding.MismatchedInlineShardingAlgorithmExpressionAndColumnException;
import org.apache.shardingsphere.sharding.exception.algorithm.sharding.ShardingAlgorithmInitializationException;

import java.util.Collection;
import java.util.Optional;
import java.util.Properties;

/**
 * Inline sharding algorithm.
 */
public final class InlineJavascriptShardingAlgorithm implements StandardShardingAlgorithm<Comparable<?>> {
    
    private static final String ALGORITHM_EXPRESSION_KEY = "algorithm-expression";
    
    private static final String ALLOW_RANGE_QUERY_KEY = "allow-range-query-with-inline-sharding";
    
    private String algorithmExpression;
    
    private boolean allowRangeQuery;
    
    @Override
    public void init(final Properties props) {
        algorithmExpression = getAlgorithmExpression(props);
        allowRangeQuery = isAllowRangeQuery(props);
    }
    
    private String getAlgorithmExpression(final Properties props) {
        String expression = props.getProperty(ALGORITHM_EXPRESSION_KEY);
        ShardingSpherePreconditions.checkState(null != expression && !expression.isEmpty(),
                () -> new ShardingAlgorithmInitializationException(getType(), "Inline sharding algorithm expression cannot be null or empty"));
        return InlineExpressionParser.handlePlaceHolder(expression.trim());
    }
    
    private boolean isAllowRangeQuery(final Properties props) {
        return Boolean.parseBoolean(props.getOrDefault(ALLOW_RANGE_QUERY_KEY, Boolean.FALSE.toString()).toString());
    }
    
    @Override
    public String doSharding(final Collection<String> availableTargetNames, final PreciseShardingValue<Comparable<?>> shardingValue) {
        Closure<?> closure = createClosure();
        Comparable<?> value = shardingValue.getValue();
        closure.setProperty(shardingValue.getColumnName(), value);
        return getTargetShardingNode(closure, shardingValue.getColumnName());
    }
    
    @Override
    public Collection<String> doSharding(final Collection<String> availableTargetNames, final RangeShardingValue<Comparable<?>> shardingValue) {
        ShardingSpherePreconditions.checkState(allowRangeQuery,
                () -> new UnsupportedSQLOperationException(String.format("Since the property of `%s` is false, inline sharding algorithm can not tackle with range query", ALLOW_RANGE_QUERY_KEY)));
        return availableTargetNames;
    }
    
    private Closure<?> createClosure() {
        Closure<?> result = new InlineExpressionParser(algorithmExpression).evaluateClosure().rehydrate(new Expando(), null, null);
        result.setResolveStrategy(Closure.DELEGATE_ONLY);
        return result;
    }
    
    private String getTargetShardingNode(final Closure<?> closure, final String columnName) {
        try {
            return closure.call().toString();
        } catch (final MissingMethodException | NullPointerException ex) {
            throw new MismatchedInlineShardingAlgorithmExpressionAndColumnException(algorithmExpression, columnName);
        }
    }
    
    @Override
    public Optional<String> getAlgorithmStructure(final String dataNodePrefix, final String shardingColumn) {
        return Optional.of(algorithmExpression.replaceFirst(dataNodePrefix, "").replaceFirst(shardingColumn, "").replaceAll(" ", ""));
    }
    
    @Override
    public String getType() {
        return "INLINE";
    }
}
