package com.lingh.complex;

import com.google.common.collect.Range;
import org.apache.shardingsphere.infra.util.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.sharding.api.sharding.complex.ComplexKeysShardingValue;
import org.apache.shardingsphere.sharding.spi.ShardingAlgorithm;
import org.apache.shardingsphere.test.util.PropertiesBuilder;
import org.apache.shardingsphere.test.util.PropertiesBuilder.Property;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

public final class ComplexInlineJavascriptShardingAlgorithmTest {

    @Test
    public void assertDoSharding() {
        Properties props = PropertiesBuilder.build(new Property("algorithm-expression", "t_order_${type % 2}_${order_id % 2}"), new Property("sharding-columns", "type,order_id"));
        ComplexInlineJavascriptShardingAlgorithm algorithm = (ComplexInlineJavascriptShardingAlgorithm) TypedSPILoader.getService(ShardingAlgorithm.class, "COMPLEX_INLINE", props);
        List<String> availableTargetNames = Arrays.asList("t_order_0_0", "t_order_0_1", "t_order_1_0", "t_order_1_1");
        Collection<String> actual = algorithm.doSharding(availableTargetNames, createComplexKeysShardingValue(Collections.singletonList(2)));
        assertTrue(1 == actual.size() && actual.contains("t_order_0_0"));
    }

    @Test
    public void assertDoShardingWithMultiValue() {
        Properties props = PropertiesBuilder.build(new Property("algorithm-expression", "t_order_${type % 2}_${order_id % 2}"), new Property("sharding-columns", "type,order_id"));
        ComplexInlineJavascriptShardingAlgorithm algorithm = (ComplexInlineJavascriptShardingAlgorithm) TypedSPILoader.getService(ShardingAlgorithm.class, "COMPLEX_INLINE", props);
        List<String> availableTargetNames = Arrays.asList("t_order_0_0", "t_order_0_1", "t_order_1_0", "t_order_1_1");
        Collection<String> actual = algorithm.doSharding(availableTargetNames, createComplexKeysShardingValue(Arrays.asList(1, 2)));
        assertTrue(actual.containsAll(availableTargetNames));
    }

    private ComplexKeysShardingValue<Comparable<?>> createComplexKeysShardingValue(final List<Comparable<?>> values) {
        Map<String, Collection<Comparable<?>>> sharingValues = new HashMap<>(2, 1);
        sharingValues.put("type", values);
        sharingValues.put("order_id", values);
        return new ComplexKeysShardingValue<>("t_order", sharingValues, Collections.emptyMap());
    }

    @Test
    public void assertDoShardingWithRangeValue() {
        Properties props = PropertiesBuilder.build(new Property("algorithm-expression", "t_order_${type % 2}_${order_id % 2}"),
                new Property("sharding-columns", "type,order_id"), new Property("allow-range-query-with-inline-sharding", Boolean.TRUE.toString()));
        ComplexInlineJavascriptShardingAlgorithm algorithm = (ComplexInlineJavascriptShardingAlgorithm) TypedSPILoader.getService(ShardingAlgorithm.class, "COMPLEX_INLINE", props);
        List<String> availableTargetNames = Arrays.asList("t_order_0_0", "t_order_0_1", "t_order_1_0", "t_order_1_1");
        Collection<String> actual = algorithm.doSharding(availableTargetNames, new ComplexKeysShardingValue<>("t_order", Collections.emptyMap(), Collections.singletonMap("type", Range.all())));
        assertTrue(actual.containsAll(availableTargetNames));
    }
}
