package stsc.distributed.hadoop.grid;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestGridHadoopStarter {

	@Rule
	public TemporaryFolder testFolder = new TemporaryFolder();

	final private Path resourceToPath(final String resourcePath) throws URISyntaxException {
		return FileSystems.getDefault().getPath(new File(TestGridHadoopStarter.class.getResource(resourcePath).toURI()).getAbsolutePath());
	}

	@Test
	public void testGridHadoopStarter() throws Exception {
		final Path tempTestFolder = FileSystems.getDefault().getPath(testFolder.getRoot().getAbsolutePath());

		final HadoopStarterSettings hadoopStarterSettings = HadoopStarterSettings.createBuilder(). //
				setOriginalDatafeedPath(resourceToPath("./test_data").toFile().getAbsolutePath()). //
				setDatafeedHdfsPath(tempTestFolder.resolve("yahoo_datafeed").toString()). //
				setHdfsOutputPath(testFolder.getRoot().getAbsolutePath()). //
				setLocalOutputPath(tempTestFolder.resolve("result_out").toFile().getAbsolutePath()). //
				setTempLocalDir(testFolder.newFolder("tmp").getAbsolutePath()). //
				build();

		final HadoopStarter hadoopStarter = new GridHadoopStarter(hadoopStarterSettings);
		Assert.assertEquals(8, hadoopStarter.searchOnHadoop().size());
	}
}
