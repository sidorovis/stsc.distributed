package stsc.distributed.hadoop.grid;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import stsc.common.BadSignalException;
import stsc.common.algorithms.BadAlgorithmException;
import stsc.common.storage.StockStorage;
import stsc.distributed.hadoop.HadoopYahooStockStorage;
import stsc.distributed.hadoop.types.SimulatorSettingsWritable;
import stsc.distributed.hadoop.types.TradingStrategyWritable;
import stsc.general.simulator.Simulator;
import stsc.general.simulator.SimulatorSettings;
import stsc.general.statistic.Metrics;
import stsc.general.strategy.TradingStrategy;

/**
 * {@link SimulatorMapper} is an implementation to {@link Mapper} with next
 * generic parameters: <br/>
 * 1. Output from Reader: <br/>
 * 1.a. ID of strategy to simulate ({@link LongWritable}); <br/>
 * 1.b. {@link SimulatorSettingsWritable} - all settings that required for
 * Simulation (on the Mapper phase); <br/>
 * 2. Output From Mapper (Input for Reducer): <br/>
 * 2.a. zero value - because we require some common key for all reducers; <br/>
 * 2.b. {@link TradingStrategyWritable} - Trading Strategy with calculated
 * metrics. <br/>
 * The goal of Mapper task is to execute {@link Simulator} and calculate
 * {@link Metrics} for the trading strategy.
 */
public final class SimulatorMapper extends Mapper<LongWritable, SimulatorSettingsWritable, LongWritable, TradingStrategyWritable> {

	private final HadoopYahooStockStorage hadoopYahooStockStorage;
	private final LongWritable zero = new LongWritable(0);

	public SimulatorMapper() throws IOException {
		this.hadoopYahooStockStorage = new HadoopYahooStockStorage();
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		hadoopYahooStockStorage.getStockStorage(context.getConfiguration());
	}

	@Override
	protected void map(LongWritable key, SimulatorSettingsWritable value, Context context) throws java.io.IOException, InterruptedException {
		final StockStorage stockStorage = hadoopYahooStockStorage.getStockStorage(context.getConfiguration());
		try {
			final SimulatorSettings settings = value.getSimulatorSettings(stockStorage);
			final Simulator simulator = new Simulator(settings);
			final Metrics metrics = simulator.getMetrics();
			final TradingStrategy tradingStrategy = new TradingStrategy(settings, metrics);
			context.write(zero, new TradingStrategyWritable(tradingStrategy));
		} catch (BadAlgorithmException | BadSignalException e) {
			throw new IOException(e.getMessage());
		}
	};
}
