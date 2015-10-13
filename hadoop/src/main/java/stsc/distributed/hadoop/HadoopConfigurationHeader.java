package stsc.distributed.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * This class store / retrieve all necessary configuration details for Hadoop.
 */
public final class HadoopConfigurationHeader {

	private static String HDFS_YAHOO_STOCK_STORAGE_PATH = "hdfs.yahoo.stock.storage.path";
	private static String HDFS_OUTPUT_FOLDER_PATH = "hdfs.output.folder.path";
	private static String HDFS_OUTPUT_FILENAME = "hdfs.output.filename";
	private static String INPUT_SPLIT_SIZE = "input.split.size";
	private static String INPUT_SPLIT_LOCATIONS = "input.split.locations";

	//

	public static void setYahooStockStoragePath(final Configuration configuration, final String value) {
		configuration.set(HDFS_YAHOO_STOCK_STORAGE_PATH, value);
	}

	public static Path getYahooStockStoragePath(final Configuration configuration) {
		return new Path(configuration.get(HDFS_YAHOO_STOCK_STORAGE_PATH));
	}

	//

	public static void setHdfsOutputFolderPath(final Configuration configuration, final String value) {
		configuration.set(HDFS_OUTPUT_FOLDER_PATH, value);
	}

	public static Path getHdfsOutputFolderPath(final Configuration configuration) {
		return new Path(configuration.get(HDFS_OUTPUT_FOLDER_PATH));
	}

	//

	public static void setHdfsOutputFilename(final Configuration configuration, final String value) {
		configuration.set(HDFS_OUTPUT_FILENAME, value);
	}

	private static Path getHdfsOutputFilename(final Configuration configuration) {
		return new Path(configuration.get(HDFS_OUTPUT_FILENAME));
	}

	public static Path getHdfsOutputFilePath(final Configuration configuration) {
		return new Path(getHdfsOutputFolderPath(configuration), getHdfsOutputFilename(configuration));
	}

	//

	public static int getInputSplitSize(final Configuration configuration) {
		return configuration.getInt(INPUT_SPLIT_SIZE, 1);
	}

	public static String[] getInputSplitLocations(final Configuration configuration) {
		return configuration.getStrings(INPUT_SPLIT_LOCATIONS, "this");
	}
}
