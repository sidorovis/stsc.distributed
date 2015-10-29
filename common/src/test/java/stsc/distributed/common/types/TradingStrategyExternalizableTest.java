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
import stsc.common.algorithms.BadAlgorithmException;
import stsc.general.simulator.SimulatorConfiguration;
import stsc.general.simulator.multistarter.genetic.GeneticList;
import stsc.general.statistic.MetricType;
import stsc.general.statistic.Metrics;
import stsc.general.strategy.TradingStrategy;
import stsc.general.testhelper.TestGeneticSimulatorSettings;
import stsc.storage.mocks.StockStorageMock;

public class TradingStrategyExternalizableTest {

	private SimulatorConfiguration getSettings() throws BadAlgorithmException {
		final GeneticList list = TestGeneticSimulatorSettings.getGeneticList();
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
	public void testTradingStrategyExternalizable() throws BadAlgorithmException, IOException, ClassNotFoundException {
		final TradingStrategy ts = new TradingStrategy(getSettings(), getMetrics());

		final PipedInputStream input = new PipedInputStream(100000);
		final PipedOutputStream output = new PipedOutputStream(input);

		final TradingStrategyExternalizable tsw = new TradingStrategyExternalizable(ts);

		final ObjectOutputStream objectOutputStream = new ObjectOutputStream(output);
		tsw.writeExternal(objectOutputStream);
		objectOutputStream.flush();

		final TradingStrategyExternalizable tswCopy = new TradingStrategyExternalizable();
		tswCopy.readExternal(new ObjectInputStream(input));
		input.close();

		final TradingStrategy tsCopy = tswCopy.getTradingStrategy(StockStorageMock.getStockStorage());
		Assert.assertEquals(ts.getAvGain(), tsCopy.getAvGain(), Settings.doubleEpsilon);
		Assert.assertEquals(ts.getSettings().stringHashCode(), tsCopy.getSettings().stringHashCode());
		Assert.assertEquals(ts.getMetrics().getIntegerMetric(MetricType.period), tsCopy.getMetrics().getIntegerMetric(MetricType.period));
	}
}
