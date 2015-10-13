package stsc.distributed.common.types;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import org.junit.Assert;
import org.junit.Test;

import stsc.common.algorithms.BadAlgorithmException;
import stsc.general.simulator.SimulatorSettings;
import stsc.general.simulator.multistarter.genetic.SimulatorSettingsGeneticList;
import stsc.general.testhelper.TestGeneticSimulatorSettings;

public class SimulatorSettingsExternalizableTest {

	@Test
	public void testSimulatorSettingsExternalizable() throws IOException, BadAlgorithmException, ClassNotFoundException {
		final SimulatorSettingsGeneticList list = TestGeneticSimulatorSettings.getGeneticList();

		for (int i = 0; i < 100; ++i) {
			final PipedInputStream input = new PipedInputStream(100000);
			final PipedOutputStream output = new PipedOutputStream(input);

			final SimulatorSettings ss = list.generateRandom();
			final SimulatorSettingsExternalizable hss = new SimulatorSettingsExternalizable(ss);

			final ObjectOutputStream objectOutputStream = new ObjectOutputStream(output);
			hss.writeExternal(objectOutputStream);
			objectOutputStream.flush();

			final SimulatorSettingsExternalizable hssCopy = new SimulatorSettingsExternalizable();
			hssCopy.readExternal(new ObjectInputStream(input));
			input.close();

			final SimulatorSettings settingsCopy = hssCopy.getSimulatorSettings(list.getStockStorage());
			Assert.assertEquals(ss.stringHashCode(), settingsCopy.stringHashCode());
		}
	}
}
