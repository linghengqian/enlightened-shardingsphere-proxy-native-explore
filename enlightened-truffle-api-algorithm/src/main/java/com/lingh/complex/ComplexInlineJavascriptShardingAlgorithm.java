package com.lingh.complex;

import groovy.lang.Closure;
import groovy.util.Expando;
import org.apache.shardingsphere.infra.util.exception.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.util.exception.external.sql.type.generic.UnsupportedSQLOperationException;
import org.apache.shardingsphere.infra.util.expr.InlineExpressionParser;
import org.apache.shardingsphere.sharding.api.sharding.complex.ComplexKeysShardingAlgorithm;
import org.apache.shardingsphere.sharding.api.sharding.complex.ComplexKeysShardingValue;
import org.apache.shardingsphere.sharding.exception.algorithm.sharding.MismatchedComplexInlineShardingAlgorithmColumnAndValueSizeException;
import org.apache.shardingsphere.sharding.exception.algorithm.sharding.ShardingAlgorithmInitializationException;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.stream.Collectors;

public final class ComplexInlineJavascriptShardingAlgorithm implements ComplexKeysShardingAlgorithm<Comparable<?>> {

    private static final String ALGORITHM_EXPRESSION_KEY = "algorithm-expression";

    private static final String SHARING_COLUMNS_KEY = "sharding-columns";

    private static final String ALLOW_RANGE_QUERY_KEY = "allow-range-query-with-inline-sharding";

    private String algorithmExpression;

    private Collection<String> shardingColumns;

    private boolean allowRangeQuery;

    @Override
    public void init(final Properties props) {
        String algorithmExpression = props.getProperty(ALGORITHM_EXPRESSION_KEY);
        ShardingSpherePreconditions.checkNotNull(algorithmExpression, () -> new ShardingAlgorithmInitializationException(getType(), "Inline sharding algorithm expression can not be null."));
        this.algorithmExpression = InlineExpressionParser.handlePlaceHolder(algorithmExpression.trim());
        String shardingColumns = props.getProperty(SHARING_COLUMNS_KEY, "");
        this.shardingColumns = shardingColumns.isEmpty() ? Collections.emptyList() : Arrays.asList(shardingColumns.split(","));
        allowRangeQuery = Boolean.parseBoolean(props.getOrDefault(ALLOW_RANGE_QUERY_KEY, Boolean.FALSE.toString()).toString());
    }

    @Override
    public Collection<String> doSharding(final Collection<String> availableTargetNames, final ComplexKeysShardingValue<Comparable<?>> shardingValue) {
        if (!shardingValue.getColumnNameAndRangeValuesMap().isEmpty()) {
            ShardingSpherePreconditions.checkState(allowRangeQuery,
                    () -> new UnsupportedSQLOperationException(String.format("Since the property of `%s` is false, inline sharding algorithm can not tackle with range query", ALLOW_RANGE_QUERY_KEY)));
            return availableTargetNames;
        }
        Map<String, Collection<Comparable<?>>> columnNameAndShardingValuesMap = shardingValue.getColumnNameAndShardingValuesMap();
        ShardingSpherePreconditions.checkState(shardingColumns.isEmpty() || shardingColumns.size() == columnNameAndShardingValuesMap.size(),
                () -> new MismatchedComplexInlineShardingAlgorithmColumnAndValueSizeException(shardingColumns.size(), columnNameAndShardingValuesMap.size()));
        Collection<Map<String, Comparable<?>>> result = new LinkedList<>();
        for (Entry<String, Collection<Comparable<?>>> entry : columnNameAndShardingValuesMap.entrySet()) {
            if (result.isEmpty()) {
                for (Comparable<?> value : entry.getValue()) {
                    Map<String, Comparable<?>> item = new HashMap<>();
                    item.put(entry.getKey(), value);
                    result.add(item);
                }
            } else {
                Collection<Map<String, Comparable<?>>> list = new LinkedList<>();
                for (Map<String, Comparable<?>> loop : result) {
                    for (Comparable<?> value : entry.getValue()) {
                        Map<String, Comparable<?>> item = new HashMap<>();
                        item.put(entry.getKey(), value);
                        item.putAll(loop);
                        list.add(item);
                    }
                }
                result = list;
            }
        }
        Collection<Map<String, Comparable<?>>> combine = result;
        return combine.stream().map(shardingValues -> {
            Closure<?> closure = new InlineExpressionParser(algorithmExpression).evaluateClosure().rehydrate(new Expando(), null, null);
            closure.setResolveStrategy(Closure.DELEGATE_ONLY);
            for (Entry<String, Comparable<?>> entry : shardingValues.entrySet()) {
                closure.setProperty(entry.getKey(), entry.getValue());
            }
            return closure.call().toString();
        }).collect(Collectors.toList());
    }

    @Override
    public String getType() {
        return "COMPLEX_INLINE";
    }
}
