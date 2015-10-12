package stsc.distributed.common.types;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import org.junit.Assert;
import org.junit.Test;

import stsc.general.simulator.multistarter.BadParameterException;
import stsc.general.simulator.multistarter.grid.SimulatorSettingsGridList;
import stsc.general.testhelper.TestGridSimulatorSettings;

public class SimulatorSettingsGridListWritableTest {

	@Test
	public void testSimulatorSettingsGridListWritable() throws IOException, BadParameterException, ClassNotFoundException {
		final SimulatorSettingsGridList list = TestGridSimulatorSettings.getGridList();

		final PipedInputStream input = new PipedInputStream(100000);
		final PipedOutputStream output = new PipedOutputStream(input);

		final SimulatorSettingsGridListWritable ssgl = new SimulatorSettingsGridListWritable(list);
		final ObjectOutputStream objectOutputStream = new ObjectOutputStream(output);
		ssgl.writeExternal(objectOutputStream);
		objectOutputStream.flush();

		final SimulatorSettingsGridListWritable ssglCopy = new SimulatorSettingsGridListWritable();
		ssglCopy.readExternal(new ObjectInputStream(input));
		input.close();

		final SimulatorSettingsGridList listCopy = ssglCopy.getGridList(list.getStockStorage());
		Assert.assertEquals(list.getPeriod().toString(), listCopy.getPeriod().toString());
		Assert.assertEquals(list.getStockInitializers().size(), listCopy.getStockInitializers().size());
		Assert.assertEquals(list.getEodInitializers().size(), listCopy.getEodInitializers().size());
	}
}
