package stsc.distributed.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import stsc.common.stocks.UnitedFormatStock;
import stsc.common.storage.StockStorage;
import stsc.storage.ThreadSafeStockStorage;

/**
 * Hadoop FS Yahoo {@link StockStorage} reader. Require full path to folder with
 * data (there is no any data / filtered_data details).
 * 
 * @mark Singleton
 */
public final class HadoopYahooStockStorage {

	private volatile StockStorage stockStorage = null;

	public synchronized StockStorage getStockStorage(Configuration configuration) throws IOException {
		final FileSystem hdfs = FileSystem.get(configuration);
		final Path stockStorageHdfsPath = HadoopConfigurationHeader.getYahooStockStoragePath(configuration);
		return getStockStorage(hdfs, stockStorageHdfsPath);
	}

	private synchronized StockStorage getStockStorage(final FileSystem hdfs, final Path path) throws IOException {
		if (stockStorage == null) {
			stockStorage = new ThreadSafeStockStorage();
			final RemoteIterator<LocatedFileStatus> fileIterator = hdfs.listFiles(path, false);
			while (fileIterator.hasNext()) {
				final LocatedFileStatus lfs = fileIterator.next();
				try (final FSDataInputStream in = hdfs.open(lfs.getPath())) {
					stockStorage.updateStock(UnitedFormatStock.readFromUniteFormatFile(in));
				}
			}
		}
		return stockStorage;
	}

}
