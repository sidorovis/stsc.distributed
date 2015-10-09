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
		{
			final HadoopSettings hadoopSettings = HadoopSettings.getInstance();
			hadoopSettings.originalDatafeedPath = resourceToPath("./test_data").toFile().getAbsolutePath();
			hadoopSettings.datafeedHdfsPath = FileSystems.getDefault().getPath(testFolder.getRoot().getAbsolutePath()).resolve("yahoo_datafeed").toString();
			hadoopSettings.tmpFolder = testFolder.newFolder("tmp").getAbsolutePath();
			hadoopSettings.outputPathOnLocal = testFolder.getRoot().getAbsolutePath();
			final HadoopStarter hadoopStarter = new GridHadoopStarter(hadoopSettings);
			Assert.assertEquals(8, hadoopStarter.searchOnHadoop().size());
		}
	}
}
