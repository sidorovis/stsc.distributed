package stsc.distributed.spark.grid;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import stsc.algorithms.Input;
import stsc.algorithms.indices.primitive.stock.Ema;
import stsc.algorithms.indices.primitive.stock.Level;
import stsc.algorithms.primitive.eod.OneSideOpenAlgorithm;
import stsc.algorithms.primitive.eod.PositionNDayMStocks;
import stsc.common.FromToPeriod;
import stsc.common.algorithms.BadAlgorithmException;
import stsc.common.storage.StockStorage;
import stsc.distributed.common.types.SimulatorSettingsExternalizable;
import stsc.general.simulator.Execution;
import stsc.general.simulator.ExecutionImpl;
import stsc.general.simulator.multistarter.AlgorithmSettingsIteratorFactory;
import stsc.general.simulator.multistarter.BadParameterException;
import stsc.general.simulator.multistarter.MpDouble;
import stsc.general.simulator.multistarter.MpInteger;
import stsc.general.simulator.multistarter.MpSubExecution;
import stsc.general.simulator.multistarter.grid.SimulatorSettingsGridFactory;
import stsc.general.simulator.multistarter.grid.SimulatorSettingsGridList;
import stsc.storage.AlgorithmsStorage;
import stsc.storage.mocks.StockStorageMock;

/**
 * This is {@link ExecutionImpl} generator / creator for the Spark Mapper (Initial Input).
 */
public final class GridRecordReader {

	public GridRecordReader() {
	}

	public List<SimulatorSettingsExternalizable> getGridList() throws IOException {
		final StockStorage stockStorage = StockStorageMock.getStockStorage();
		final ArrayList<SimulatorSettingsExternalizable> result = new ArrayList<>();
		for (Execution ss : getDefaultSimulatorSettingsGridList(stockStorage)) {
			result.add(new SimulatorSettingsExternalizable(ss));
		}
		return result;
	}

	private SimulatorSettingsGridList getDefaultSimulatorSettingsGridList(final StockStorage stockStorage) throws IOException {
		try {
			final FromToPeriod period = new FromToPeriod("01-01-2013", "01-01-2014");
			final SimulatorSettingsGridFactory factory = new SimulatorSettingsGridFactory(stockStorage, period);
			fillFactory(factory);
			return factory.getList();
		} catch (ParseException | BadParameterException | BadAlgorithmException e) {
			throw new IOException(e.getMessage());
		}
	}

	private static void fillFactory(SimulatorSettingsGridFactory settings) throws BadParameterException, BadAlgorithmException {
		settings.addStock("in", algoStockName(Input.class.getSimpleName()), "e", Arrays.asList(new String[] { "open", "close" }));
		settings.addStock("ema", algoStockName(Ema.class.getSimpleName()),
				new AlgorithmSettingsIteratorFactory().add(new MpDouble("P", 0.1, 0.6, 0.5)).add(new MpSubExecution("", "in")));
		settings.addStock("level", algoStockName("." + Level.class.getSimpleName()),
				new AlgorithmSettingsIteratorFactory().add(new MpDouble("f", 15.0, 20.0, 5)).add(new MpSubExecution("", Arrays.asList(new String[] { "ema" }))));
		settings.addEod("os", algoEodName(OneSideOpenAlgorithm.class.getSimpleName()), "side", Arrays.asList(new String[] { "long", "short" }));

		final AlgorithmSettingsIteratorFactory factoryPositionSide = new AlgorithmSettingsIteratorFactory();
		factoryPositionSide.add(new MpSubExecution("", Arrays.asList(new String[] { "in", "level" })));
		factoryPositionSide.add(new MpInteger("n", 1, 32, 32));
		factoryPositionSide.add(new MpInteger("m", 1, 32, 32));
		factoryPositionSide.add(new MpDouble("ps", 50000.0, 200001.0, 160000.0));
		settings.addEod("pnm", algoEodName(PositionNDayMStocks.class.getSimpleName()), factoryPositionSide);
	}

	private static String algoStockName(String aname) throws BadAlgorithmException {
		return AlgorithmsStorage.getInstance().getStock(aname).getName();
	}

	private static String algoEodName(String aname) throws BadAlgorithmException {
		return AlgorithmsStorage.getInstance().getEod(aname).getName();
	}

}
