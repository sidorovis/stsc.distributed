package stsc.distributed.hadoop.types;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputByteBuffer;
import org.junit.Assert;
import org.junit.Test;

import stsc.common.Settings;
import stsc.common.algorithms.BadAlgorithmException;
import stsc.general.simulator.SimulatorSettings;
import stsc.general.simulator.multistarter.genetic.SimulatorSettingsGeneticList;
import stsc.general.statistic.MetricType;
import stsc.general.statistic.Metrics;
import stsc.general.strategy.TradingStrategy;
import stsc.general.testhelper.TestGeneticSimulatorSettings;
import stsc.storage.mocks.StockStorageMock;

public class TradingStrategyWritableTest {

	private SimulatorSettings getSettings() throws BadAlgorithmException {
		final SimulatorSettingsGeneticList list = TestGeneticSimulatorSettings.getGeneticList();
		return list.generateRandom();
	}

	private Metrics getMetrics() {
		final Map<MetricType, Double> listDouble = new HashMap<>();
		listDouble.put(MetricType.avGain, 10.45);
		listDouble.put(MetricType.avWinAvLoss, 62.13);
		final Map<MetricType, Integer> listInteger = new HashMap<>();
		listInteger.put(MetricType.period, 16);
		return new Metrics(listDouble, listInteger);
	}

	@Test
	public void testTradingStrategyWritable() throws BadAlgorithmException, IOException {
		final TradingStrategy ts = new TradingStrategy(getSettings(), getMetrics());

		final DataOutputByteBuffer output = new DataOutputByteBuffer();
		final DataInputByteBuffer input = new DataInputByteBuffer();

		final TradingStrategyWritable tsw = new TradingStrategyWritable(ts);

		tsw.write(output);
		input.reset(output.getData());

		final TradingStrategyWritable tswCopy = new TradingStrategyWritable();
		tswCopy.readFields(input);

		final TradingStrategy tsCopy = tswCopy.getTradingStrategy(StockStorageMock.getStockStorage());
		Assert.assertEquals(ts.getAvGain(), tsCopy.getAvGain(), Settings.doubleEpsilon);
		Assert.assertEquals(ts.getSettings().stringHashCode(), tsCopy.getSettings().stringHashCode());
		Assert.assertEquals(ts.getMetrics().getIntegerMetric(MetricType.period), tsCopy.getMetrics().getIntegerMetric(MetricType.period));
	}
}
