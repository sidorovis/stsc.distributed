package stsc.distributed.hadoop.grid;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import stsc.algorithms.Input;
import stsc.algorithms.indices.primitive.stock.Ema;
import stsc.algorithms.indices.primitive.stock.Level;
import stsc.algorithms.primitive.eod.OneSideOpenAlgorithm;
import stsc.algorithms.primitive.eod.PositionNDayMStocks;
import stsc.common.FromToPeriod;
import stsc.common.algorithms.BadAlgorithmException;
import stsc.common.storage.StockStorage;
import stsc.distributed.hadoop.HadoopYahooStockStorage;
import stsc.distributed.hadoop.types.SimulatorSettingsWritable;
import stsc.general.simulator.SimulatorSettings;
import stsc.general.simulator.multistarter.AlgorithmSettingsIteratorFactory;
import stsc.general.simulator.multistarter.BadParameterException;
import stsc.general.simulator.multistarter.MpDouble;
import stsc.general.simulator.multistarter.MpInteger;
import stsc.general.simulator.multistarter.MpSubExecution;
import stsc.general.simulator.multistarter.grid.SimulatorSettingsGridFactory;
import stsc.general.simulator.multistarter.grid.SimulatorSettingsGridList;
import stsc.storage.AlgorithmsStorage;

/**
 * This actually only example how it is possible to create one layered
 * distributed {@link SimulatorSettings} (for Grid (brute-force) search). <br/>
 * Simulator Settings are hard-coded.
 */
public final class GridRecordReader extends RecordReader<LongWritable, SimulatorSettingsWritable> {

	private long size;
	private long id = 0;
	private Iterator<SimulatorSettings> iterator;
	private SimulatorSettings current;
	private boolean finished;

	public GridRecordReader(Configuration configuration) throws IOException {
		final StockStorage stockStorage = new HadoopYahooStockStorage().getStockStorage(configuration);
		final SimulatorSettingsGridList list = getGridList(stockStorage);
		this.iterator = list.iterator();
		this.size = list.size();
		this.finished = !iterator.hasNext();
		this.current = iterator.next();
	}

	public SimulatorSettingsGridList getGridList(final StockStorage stockStorage) throws IOException {
		return getDefaultSimulatorSettingsGridList(stockStorage);
	}

	private SimulatorSettingsGridList getDefaultSimulatorSettingsGridList(final StockStorage stockStorage) throws IOException {
		try {
			final FromToPeriod period = new FromToPeriod("01-01-2013", "01-01-2014");
			final SimulatorSettingsGridFactory factory = new SimulatorSettingsGridFactory(stockStorage, period);
			fillFactory(period, factory);
			return factory.getList();
		} catch (ParseException | BadParameterException | BadAlgorithmException e) {
			throw new IOException(e.getMessage());
		}
	}

	private static void fillFactory(FromToPeriod period, SimulatorSettingsGridFactory settings) throws BadParameterException, BadAlgorithmException {
		settings.addStock("in", algoStockName(Input.class.getSimpleName()), "e", Arrays.asList(new String[] { "open", "close" }));
		settings.addStock("ema", algoStockName(Ema.class.getSimpleName()),
				new AlgorithmSettingsIteratorFactory(period).add(new MpDouble("P", 0.1, 0.6, 0.5)).add(new MpSubExecution("", "in")));
		settings.addStock("level", algoStockName("." + Level.class.getSimpleName()),
				new AlgorithmSettingsIteratorFactory(period).add(new MpDouble("f", 15.0, 20.0, 5)).add(new MpSubExecution("", Arrays.asList(new String[] { "ema" }))));
		settings.addEod("os", algoEodName(OneSideOpenAlgorithm.class.getSimpleName()), "side", Arrays.asList(new String[] { "long", "short" }));

		final AlgorithmSettingsIteratorFactory factoryPositionSide = new AlgorithmSettingsIteratorFactory(period);
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

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		// DO NOTHING
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return !finished;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return new LongWritable(current.getId());
	}

	@Override
	public SimulatorSettingsWritable getCurrentValue() throws IOException, InterruptedException {
		if (iterator.hasNext()) {
			final SimulatorSettings result = current;
			current = iterator.next();
			id = current.getId();
			return new SimulatorSettingsWritable(result);
		} else {
			finished = true;
			return new SimulatorSettingsWritable(current);
		}
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return ((float) id) / size;
	}

	@Override
	public void close() throws IOException {
		iterator = null;
		current = null;
	}
}
