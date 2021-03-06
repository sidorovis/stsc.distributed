package stsc.distributed.hadoop.grid;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import stsc.common.algorithms.BadAlgorithmException;
import stsc.common.storage.StockStorage;
import stsc.distributed.hadoop.HadoopConfigurationHeader;
import stsc.distributed.hadoop.HadoopYahooStockStorage;
import stsc.distributed.hadoop.types.MetricsWritable;
import stsc.distributed.hadoop.types.SimulatorSettingsWritable;
import stsc.distributed.hadoop.types.TradingStrategyWritable;
import stsc.general.strategy.TradingStrategy;

/**
 * 1) Copy Datafeed from local to hdfs (<local>/"./test_data/" ->
 * <hdfs>/"./yahoo_datafeed/"); <br/>
 * 2) Start separated tasks; <br/>
 * 3) Load results from Hdfs.
 */

public final class GridHadoopStarter extends Configured implements Tool, HadoopStarter {

	private final HadoopStarterSettings hadoopStarterSettings;
	private final List<TradingStrategy> tradingStrategies = new ArrayList<TradingStrategy>();

	public GridHadoopStarter(final HadoopStarterSettings hadoopStarterSettings) throws IOException {
		this.hadoopStarterSettings = hadoopStarterSettings;
	}

	@Override
	public List<TradingStrategy> searchOnHadoop() throws Exception {
		String[] args = new String[0];
		ToolRunner.run(this, args);
		return tradingStrategies;
	}

	@Override
	public int run(String[] args) throws Exception {
		this.getConf().set(MRConfig.LOCAL_DIR, hadoopStarterSettings.getTempLocalDir());
		HadoopConfigurationHeader.setYahooStockStoragePath(this.getConf(), hadoopStarterSettings.getDatafeedHdfsPath());
		HadoopConfigurationHeader.setHdfsOutputFolderPath(this.getConf(), hadoopStarterSettings.getHdfsOutputPath());
		HadoopConfigurationHeader.setHdfsOutputFilename(this.getConf(), hadoopStarterSettings.getOutputFileName());

		final Job job = Job.getInstance(this.getConf());

		if (hadoopStarterSettings.isCopyOriginalDatafeedPath()) {
			checkAndCopyDatafeed(hadoopStarterSettings.getOriginalDatafeedPath(), hadoopStarterSettings.getDatafeedHdfsPath());
		}
		job.setJobName(GridHadoopStarter.class.getSimpleName());
		job.setJarByClass(GridHadoopStarter.class);

		job.setMapperClass(SimulatorMapper.class);
		job.setReducerClass(SimulatorReducer.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(TradingStrategyWritable.class);

		job.setOutputKeyClass(SimulatorSettingsWritable.class);
		job.setOutputValueClass(MetricsWritable.class);

		job.setInputFormatClass(GridInputFormat.class);
		job.setOutputFormatClass(GridOutputFormat.class);

		job.waitForCompletion(true);
		loadTradingStrategies();
		if (hadoopStarterSettings.isCopyAnswerToLocal()) {
			copyAnswerToLocal();
		}
		return 0;
	}

	private void loadTradingStrategies() throws IOException, BadAlgorithmException {
		final FileSystem hdfs = FileSystem.get(this.getConf());
		final Path pathToOutputFile = HadoopConfigurationHeader.getHdfsOutputFilePath(this.getConf());
		final StockStorage stockStorage = new HadoopYahooStockStorage().getStockStorage(this.getConf());
		if (hdfs.exists(pathToOutputFile)) {
			final FSDataInputStream fileIn = hdfs.open(pathToOutputFile);
			final int size = fileIn.readInt();
			for (int i = 0; i < size; ++i) {
				final TradingStrategyWritable tsw = new TradingStrategyWritable();
				tsw.readFields(fileIn);
				tradingStrategies.add(tsw.getTradingStrategy(stockStorage));
			}
		}
	}

	private void checkAndCopyDatafeed(String localPath, String hdfsYahooDatafeedPath) throws IOException {
		final FileSystem hdfs = FileSystem.get(this.getConf());
		final Path path = new Path(hdfsYahooDatafeedPath);
		if (!hdfs.exists(path)) {
			hdfs.mkdirs(path);
			File folder = new File(localPath);
			File[] listOfFiles = folder.listFiles();
			for (File file : listOfFiles) {
				hdfs.copyFromLocalFile(new Path(file.getPath()), new Path(path, file.getName()));
			}
		}
	}

	private void copyAnswerToLocal() throws IllegalArgumentException, IOException {
		final FileSystem hdfs = FileSystem.get(this.getConf());
		final Path out = new Path(hadoopStarterSettings.getHdfsOutputPath(), hadoopStarterSettings.getOutputFileName());
		final Path localOut = new Path(hadoopStarterSettings.getLocalOutputPath(), hadoopStarterSettings.getOutputFileName());
		if (hdfs.exists(out)) {
			hdfs.copyToLocalFile(true, out, localOut, true);
		}
	}
}
