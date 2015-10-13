package stsc.distributed.common.types;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import stsc.common.algorithms.BadAlgorithmException;
import stsc.general.simulator.multistarter.BadParameterException;
import stsc.general.simulator.multistarter.genetic.GeneticExecutionInitializer;
import stsc.general.simulator.multistarter.genetic.SimulatorSettingsGeneticList;
import stsc.general.testhelper.TestGeneticSimulatorSettings;

public class SimulatorSettingsGeneticListExternalizableTest {

	@Test
	public void testSimulatorSettingsGeneticListExternalizable() throws BadAlgorithmException, IOException, BadParameterException, ClassNotFoundException {
		final SimulatorSettingsGeneticList list = TestGeneticSimulatorSettings.getGeneticList();

		final PipedInputStream input = new PipedInputStream(100000);
		final PipedOutputStream output = new PipedOutputStream(input);

		final SimulatorSettingsGeneticListExternalizable ssgl = new SimulatorSettingsGeneticListExternalizable(list);
		final ObjectOutputStream objectOutputStream = new ObjectOutputStream(output);
		ssgl.writeExternal(objectOutputStream);
		objectOutputStream.flush();

		final SimulatorSettingsGeneticListExternalizable ssglCopy = new SimulatorSettingsGeneticListExternalizable();
		ssglCopy.readExternal(new ObjectInputStream(input));
		input.close();

		final SimulatorSettingsGeneticList listCopy = ssglCopy.getGeneticList(list.getStockStorage());

		final List<GeneticExecutionInitializer> stocks = list.getStockInitializers();
		final List<GeneticExecutionInitializer> stocksCopy = listCopy.getStockInitializers();
		final List<GeneticExecutionInitializer> eods = list.getEodInitializers();
		final List<GeneticExecutionInitializer> eodsCopy = listCopy.getEodInitializers();

		Assert.assertEquals(stocks.size(), stocksCopy.size());
		Assert.assertEquals(eods.size(), eodsCopy.size());

		Assert.assertEquals(stocks.get(0).algorithmName, stocksCopy.get(0).algorithmName);
		Assert.assertEquals(stocks.get(1).algorithmName, stocksCopy.get(1).algorithmName);
	}
}
