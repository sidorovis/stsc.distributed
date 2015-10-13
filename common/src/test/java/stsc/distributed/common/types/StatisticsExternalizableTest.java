package stsc.distributed.common.types;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import stsc.common.Settings;
import stsc.general.statistic.MetricType;
import stsc.general.statistic.Metrics;

public class StatisticsExternalizableTest {

	@Test
	public void testStatisticsExternalizable() throws IOException, ClassNotFoundException {
		final Map<MetricType, Double> doubleList = new HashMap<>();
		doubleList.put(MetricType.avGain, 10.45);
		doubleList.put(MetricType.avWinAvLoss, 62.13);
		final Map<MetricType, Integer> integerList = new HashMap<>();
		integerList.put(MetricType.period, 16);
		final Metrics s = new Metrics(doubleList, integerList);
		Assert.assertEquals(10.45, s.getMetric(MetricType.avGain), Settings.doubleEpsilon);
		Assert.assertEquals(62.13, s.getMetric(MetricType.avWinAvLoss), Settings.doubleEpsilon);
		Assert.assertEquals(16, s.getIntegerMetric(MetricType.period).intValue());

		final PipedInputStream input = new PipedInputStream(100000);
		final PipedOutputStream output = new PipedOutputStream(input);

		final MetricsExternalizable sw = new MetricsExternalizable(s);

		final ObjectOutputStream objectOutputStream = new ObjectOutputStream(output);
		sw.writeExternal(objectOutputStream);
		objectOutputStream.flush();

		final MetricsExternalizable swCopy = new MetricsExternalizable();
		swCopy.readExternal(new ObjectInputStream(input));

		final Metrics sCopy = swCopy.getMetrics();

		Assert.assertEquals(10.45, sCopy.getMetric(MetricType.avGain), Settings.doubleEpsilon);
		Assert.assertEquals(62.13, sCopy.getMetric(MetricType.avWinAvLoss), Settings.doubleEpsilon);
		Assert.assertEquals(16, sCopy.getIntegerMetric(MetricType.period).intValue());
	}
}
