package com.lingh.hint;

import org.apache.shardingsphere.infra.util.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.sharding.api.sharding.hint.HintShardingValue;
import org.apache.shardingsphere.sharding.spi.ShardingAlgorithm;
import org.apache.shardingsphere.test.util.PropertiesBuilder;
import org.apache.shardingsphere.test.util.PropertiesBuilder.Property;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

public final class HintInlineJavascriptShardingAlgorithmTest {
    
    private HintInlineJavascriptShardingAlgorithm hintInlineShardingAlgorithm;
    
    private HintInlineJavascriptShardingAlgorithm hintInlineShardingAlgorithmDefault;
    
    @Before
    public void setUp() {
        hintInlineShardingAlgorithm = (HintInlineJavascriptShardingAlgorithm) TypedSPILoader.getService(ShardingAlgorithm.class,
                "HINT_INLINE", PropertiesBuilder.build(new Property("algorithm-expression", "t_order_$->{value % 4}")));
        hintInlineShardingAlgorithmDefault = new HintInlineJavascriptShardingAlgorithm();
        hintInlineShardingAlgorithmDefault.init(new Properties());
    }
    
    @Test
    public void assertDoShardingWithSingleValueOfDefault() {
        List<String> availableTargetNames = Arrays.asList("t_order_0", "t_order_1", "t_order_2", "t_order_3");
        HintShardingValue<Comparable<?>> shardingValue = new HintShardingValue<>("t_order", "order_id", Collections.singleton("t_order_0"));
        Collection<String> actual = hintInlineShardingAlgorithmDefault.doSharding(availableTargetNames, shardingValue);
        assertTrue(actual.contains("t_order_0"));
    }
    
    @Test
    public void assertDoShardingWithSingleValue() {
        List<String> availableTargetNames = Arrays.asList("t_order_0", "t_order_1", "t_order_2", "t_order_3");
        HintShardingValue<Comparable<?>> shardingValue = new HintShardingValue<>("t_order", "order_id", Collections.singleton(4));
        Collection<String> actual = hintInlineShardingAlgorithm.doSharding(availableTargetNames, shardingValue);
        assertTrue(actual.contains("t_order_0"));
    }
    
    @Test
    public void assertDoShardingWithMultiValues() {
        List<String> availableTargetNames = Arrays.asList("t_order_0", "t_order_1", "t_order_2", "t_order_3");
        HintShardingValue<Comparable<?>> shardingValue = new HintShardingValue<>("t_order", "order_id", Arrays.asList(1, 2, 3, 4));
        Collection<String> actual = hintInlineShardingAlgorithm.doSharding(availableTargetNames, shardingValue);
        assertTrue(actual.containsAll(availableTargetNames));
    }
}
