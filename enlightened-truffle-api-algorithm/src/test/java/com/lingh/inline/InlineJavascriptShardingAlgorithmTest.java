package com.lingh.inline;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import org.apache.shardingsphere.infra.datanode.DataNodeInfo;
import org.apache.shardingsphere.infra.util.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.sharding.api.sharding.standard.PreciseShardingValue;
import org.apache.shardingsphere.sharding.api.sharding.standard.RangeShardingValue;
import org.apache.shardingsphere.sharding.exception.algorithm.sharding.MismatchedInlineShardingAlgorithmExpressionAndColumnException;
import org.apache.shardingsphere.sharding.spi.ShardingAlgorithm;
import org.apache.shardingsphere.test.util.PropertiesBuilder;
import org.apache.shardingsphere.test.util.PropertiesBuilder.Property;
import org.junit.Before;
import org.junit.Test;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public final class InlineJavascriptShardingAlgorithmTest {
    
    private static final DataNodeInfo DATA_NODE_INFO = new DataNodeInfo("t_order_", 1, '0');
    
    private InlineJavascriptShardingAlgorithm inlineShardingAlgorithm;
    
    private InlineJavascriptShardingAlgorithm inlineShardingAlgorithmWithSimplified;
    
    private InlineJavascriptShardingAlgorithm negativeNumberInlineShardingAlgorithm;
    
    @Before
    public void setUp() {
        inlineShardingAlgorithm = (InlineJavascriptShardingAlgorithm) TypedSPILoader.getService(ShardingAlgorithm.class, "INLINE",
                PropertiesBuilder.build(new Property("algorithm-expression", "t_order_$->{order_id % 4}"), new Property("allow-range-query-with-inline-sharding", Boolean.TRUE.toString())));
        inlineShardingAlgorithmWithSimplified = (InlineJavascriptShardingAlgorithm) TypedSPILoader.getService(ShardingAlgorithm.class, "INLINE",
                PropertiesBuilder.build(new Property("algorithm-expression", "t_order_${order_id % 4}")));
        negativeNumberInlineShardingAlgorithm = (InlineJavascriptShardingAlgorithm) TypedSPILoader.getService(ShardingAlgorithm.class, "INLINE",
                PropertiesBuilder.build(new Property("algorithm-expression", "t_order_$->{(order_id % 4).abs()}"), new Property("allow-range-query-with-inline-sharding", Boolean.TRUE.toString())));
    }
    
    @Test(expected = MismatchedInlineShardingAlgorithmExpressionAndColumnException.class)
    public void assertDoSharding() {
        List<String> availableTargetNames = Arrays.asList("t_order_0", "t_order_1", "t_order_2", "t_order_3");
        assertThat(inlineShardingAlgorithm.doSharding(availableTargetNames, new PreciseShardingValue<>("t_order", "order_id", DATA_NODE_INFO, 0)), is("t_order_0"));
        inlineShardingAlgorithm.doSharding(availableTargetNames, new PreciseShardingValue<>("t_order", "non_existent_column1", DATA_NODE_INFO, 0));
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void assertDoShardingWithRangeShardingConditionValue() {
        List<String> availableTargetNames = Arrays.asList("t_order_0", "t_order_1", "t_order_2", "t_order_3");
        Collection<String> actual = inlineShardingAlgorithm.doSharding(availableTargetNames, new RangeShardingValue<>("t_order", "order_id", DATA_NODE_INFO, mock(Range.class)));
        assertTrue(actual.containsAll(availableTargetNames));
    }
    
    @Test
    public void assertDoShardingWithNonExistNodes() {
        List<String> availableTargetNames = Arrays.asList("t_order_0", "t_order_1");
        assertThat(inlineShardingAlgorithm.doSharding(availableTargetNames, new PreciseShardingValue<>("t_order", "order_id", DATA_NODE_INFO, 0)), is("t_order_0"));
        assertThat(inlineShardingAlgorithmWithSimplified.doSharding(availableTargetNames, new PreciseShardingValue<>("t_order", "order_id", DATA_NODE_INFO, 0)), is("t_order_0"));
    }
    
    @Test
    public void assertDoShardingWithNegative() {
        List<String> availableTargetNames = Lists.newArrayList("t_order_0", "t_order_1", "t_order_2", "t_order_3");
        assertThat(negativeNumberInlineShardingAlgorithm.doSharding(availableTargetNames, new PreciseShardingValue<>("t_order", "order_id", DATA_NODE_INFO, -1)), is("t_order_1"));
        assertThat(negativeNumberInlineShardingAlgorithm.doSharding(availableTargetNames, new PreciseShardingValue<>("t_order", "order_id", DATA_NODE_INFO, -4)), is("t_order_0"));
    }
    
    @Test
    public void assertDoShardingWithLargeValues() {
        List<String> availableTargetNames = Lists.newArrayList("t_order_0", "t_order_1", "t_order_2", "t_order_3");
        assertThat(inlineShardingAlgorithm.doSharding(availableTargetNames,
                new PreciseShardingValue<>("t_order", "order_id", DATA_NODE_INFO, 787694822390497280L)), is("t_order_0"));
        assertThat(inlineShardingAlgorithm.doSharding(availableTargetNames,
                new PreciseShardingValue<>("t_order", "order_id", DATA_NODE_INFO, new BigInteger("787694822390497280787694822390497280"))), is("t_order_0"));
        assertThat(inlineShardingAlgorithmWithSimplified.doSharding(availableTargetNames,
                new PreciseShardingValue<>("t_order", "order_id", DATA_NODE_INFO, 787694822390497280L)), is("t_order_0"));
        assertThat(inlineShardingAlgorithmWithSimplified.doSharding(availableTargetNames,
                new PreciseShardingValue<>("t_order", "order_id", DATA_NODE_INFO, new BigInteger("787694822390497280787694822390497280"))), is("t_order_0"));
    }
}
