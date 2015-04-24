package stsc.distributed.hadoop.types;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputByteBuffer;
import org.junit.Assert;
import org.junit.Test;

import stsc.common.Settings;
import stsc.general.statistic.Metrics;

public class StatisticsWritableTest {

	@Test
	public void testStatisticsWritable() throws IOException {
		final Map<String, Double> doubleList = new HashMap<>();
		doubleList.put("avGain", 10.45);
		doubleList.put("avWinAvLoss", 62.13);
		final Map<String, Integer> integerList = new HashMap<>();
		integerList.put("period", 16);
		final Metrics s = new Metrics(doubleList, integerList);
		Assert.assertEquals(10.45, s.getMetric("avGain"), Settings.doubleEpsilon);
		Assert.assertEquals(62.13, s.getMetric("avWinAvLoss"), Settings.doubleEpsilon);
		Assert.assertEquals(16, s.getIntegerMetric("period").intValue());

		final DataOutputByteBuffer output = new DataOutputByteBuffer();
		final DataInputByteBuffer input = new DataInputByteBuffer();

		final MetricsWritable sw = new MetricsWritable(s);

		sw.write(output);
		input.reset(output.getData());

		final MetricsWritable swCopy = new MetricsWritable();
		swCopy.readFields(input);

		final Metrics sCopy = swCopy.getMetrics();

		Assert.assertEquals(10.45, sCopy.getMetric("avGain"), Settings.doubleEpsilon);
		Assert.assertEquals(62.13, sCopy.getMetric("avWinAvLoss"), Settings.doubleEpsilon);
		Assert.assertEquals(16, sCopy.getIntegerMetric("period").intValue());
	}
}
