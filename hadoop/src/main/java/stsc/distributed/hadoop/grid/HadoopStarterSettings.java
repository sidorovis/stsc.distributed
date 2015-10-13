package stsc.distributed.hadoop.grid;

import org.apache.hadoop.mapreduce.MRConfig;

/**
 * This is set of setting for hadoop search. Contain all necessary stetings:
 * folder where data situated in the beginning / folder where it should be
 * placed on HDFS / output folder output file name.
 */
final class HadoopStarterSettings {

	public final static class Builder {

		/**
		 * Original folder where datafeed situated.
		 */
		private String originalDatafeedPath = "./data/";
		/**
		 * This is a path to the Yahoo datafeed Path on HDFS (after copy). I
		 * guess that better to have it in relative format.
		 */
		private String datafeedHdfsPath = "./yahoo_datafeed/";

		/**
		 * This is a path where output results would be stored.
		 */
		private String hdfsOutputPath = "./output_data/";

		/**
		 * This is a file where output would be situated.
		 */
		private String outputFileName = "output.txt";

		/**
		 * This is a path on local file system where output would be stored.
		 */
		private String localOutputPath = "./";

		/**
		 * This folder would be used to configure {@link MRConfig#LOCAL_DIR}
		 * variable.
		 */
		private String tempLocalDir;

		/**
		 * Should we copy original yahoo datafeed to HDFS or it is already
		 * there.
		 */
		private boolean copyOriginalDatafeedPath = true;

		/**
		 * Should we copy original answer file to local filesystem.
		 */
		private boolean copyAnswerToLocal = true;

		public Builder() {

		}

		public Builder setOriginalDatafeedPath(String originalDatafeedPath) {
			this.originalDatafeedPath = originalDatafeedPath;
			return this;
		}

		public Builder setDatafeedHdfsPath(String datafeedHdfsPath) {
			this.datafeedHdfsPath = datafeedHdfsPath;
			return this;
		}

		public Builder setHdfsOutputPath(String hdfsOutputPath) {
			this.hdfsOutputPath = hdfsOutputPath;
			return this;
		}

		public Builder setOutputFileName(String outputFileName) {
			this.outputFileName = outputFileName;
			return this;
		}

		public Builder setLocalOutputPath(String localOutputPath) {
			this.localOutputPath = localOutputPath;
			return this;
		}

		public Builder setTempLocalDir(String tempLocalDir) {
			this.tempLocalDir = tempLocalDir;
			return this;
		}

		public Builder setCopyOriginalDatafeedPath(boolean copyOriginalDatafeedPath) {
			this.copyOriginalDatafeedPath = copyOriginalDatafeedPath;
			return this;
		}

		// build

		public HadoopStarterSettings build() {
			return new HadoopStarterSettings(this);
		}

	}

	private final String originalDatafeedPath;
	private final String datafeedHdfsPath;
	private final String hdfsOutputPath;
	private final String outputFileName;
	private final String localOutputPath;
	private final String tempLocalDir;
	private final boolean copyOriginalDatafeedPath;
	private final boolean copyAnswerToLocal;

	public static Builder createBuilder() {
		return new Builder();
	}

	public HadoopStarterSettings(final Builder b) {
		this.originalDatafeedPath = b.originalDatafeedPath;
		this.datafeedHdfsPath = b.datafeedHdfsPath;
		this.hdfsOutputPath = b.hdfsOutputPath;
		this.outputFileName = b.outputFileName;
		this.localOutputPath = b.localOutputPath;
		this.tempLocalDir = b.tempLocalDir;
		this.copyOriginalDatafeedPath = b.copyOriginalDatafeedPath;
		this.copyAnswerToLocal = b.copyAnswerToLocal;
	}

	public String getOriginalDatafeedPath() {
		return originalDatafeedPath;
	}

	public String getDatafeedHdfsPath() {
		return datafeedHdfsPath;
	}

	public String getHdfsOutputPath() {
		return hdfsOutputPath;
	}

	public String getOutputFileName() {
		return outputFileName;
	}

	public String getLocalOutputPath() {
		return localOutputPath;
	}

	public String getTempLocalDir() {
		return tempLocalDir;
	}

	public boolean isCopyOriginalDatafeedPath() {
		return copyOriginalDatafeedPath;
	}

	public boolean isCopyAnswerToLocal() {
		return copyAnswerToLocal;
	}

}
