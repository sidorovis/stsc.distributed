package stsc.distributed.hadoop.types;

import java.io.IOException;

import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputByteBuffer;
import org.junit.Assert;
import org.junit.Test;

import stsc.common.algorithms.BadAlgorithmException;
import stsc.general.simulator.Execution;
import stsc.general.simulator.multistarter.genetic.SimulatorSettingsGeneticListImpl;
import stsc.general.testhelper.TestGeneticSimulatorSettings;

public class SimulatorSettingsWritableTest {

	@Test
	public void testHadoopSimulatorSettings() throws IOException, BadAlgorithmException {
		final SimulatorSettingsGeneticListImpl list = TestGeneticSimulatorSettings.getGeneticList();

		for (int i = 0; i < 100; ++i) {
			final DataOutputByteBuffer output = new DataOutputByteBuffer();
			final DataInputByteBuffer input = new DataInputByteBuffer();

			final Execution ss = list.generateRandom();
			final SimulatorSettingsWritable hss = new SimulatorSettingsWritable(ss);

			hss.write(output);
			input.reset(output.getData());

			final SimulatorSettingsWritable hssCopy = new SimulatorSettingsWritable();
			hssCopy.readFields(input);

			final Execution settingsCopy = hssCopy.getSimulatorSettings(list.getStockStorage());
			Assert.assertEquals(ss.stringHashCode(), settingsCopy.stringHashCode());
		}
	}
}
