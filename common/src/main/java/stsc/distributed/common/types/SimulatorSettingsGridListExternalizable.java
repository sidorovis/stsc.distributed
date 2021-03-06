package stsc.distributed.common.types;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import stsc.common.FromToPeriod;
import stsc.common.storage.StockStorage;
import stsc.general.simulator.multistarter.AlgorithmConfigurationSet;
import stsc.general.simulator.multistarter.AlgorithmConfigurationSetImpl;
import stsc.general.simulator.multistarter.BadParameterException;
import stsc.general.simulator.multistarter.MpDouble;
import stsc.general.simulator.multistarter.MpInteger;
import stsc.general.simulator.multistarter.MpNumberIterator;
import stsc.general.simulator.multistarter.MpString;
import stsc.general.simulator.multistarter.MpSubExecution;
import stsc.general.simulator.multistarter.MpTextIterator;
import stsc.general.simulator.multistarter.ParameterList;
import stsc.general.simulator.multistarter.grid.AlgorithmConfigurationSetGridGenerator;
import stsc.general.simulator.multistarter.grid.GridExecutionInitializer;
import stsc.general.simulator.multistarter.grid.SimulatorSettingsGridFactory;
import stsc.general.simulator.multistarter.grid.SimulatorSettingsGridList;

/**
 * This is implementation for {@link Externalizable} of {@link SimulatorSettingsGridList}.
 */
public final class SimulatorSettingsGridListExternalizable extends MapEasyExternalizable {

	private static final String PERIOD_FROM = "periodFrom";
	private static final String PERIOD_TO = "periodTo";

	private static final String STOCK_PREFIX = "stockInit.";
	private static final String EOD_PREFIX = "eodInit.";

	private static final String INITIALIZERS_SIZE = "size";

	private static final String EXEC_NAME = "exec";
	private static final String ALGO_NAME = "algo";

	// will be filled in the middle of generating
	private FromToPeriod period;

	// for reading
	public SimulatorSettingsGridListExternalizable() {
	}

	// List -> Writable
	public SimulatorSettingsGridListExternalizable(final SimulatorSettingsGridList list) {
		this();
		saveList(list);
	}

	private void saveList(SimulatorSettingsGridList list) {
		longs.put(PERIOD_FROM, list.getPeriod().getFrom().getTime());
		longs.put(PERIOD_TO, list.getPeriod().getTo().getTime());
		saveExecutions(list.getStockInitializers(), STOCK_PREFIX);
		saveExecutions(list.getEodInitializers(), EOD_PREFIX);
	}

	private void saveExecutions(List<GridExecutionInitializer> initializers, String prefix) {
		integers.put(prefix + INITIALIZERS_SIZE, initializers.size());
		for (int i = 0; i < initializers.size(); ++i) {
			saveInitializer(initializers.get(i), prefix, i);
		}
	}

	private void saveInitializer(GridExecutionInitializer initializer, String prefix, int index) {
		final String prefixExec = createExecPrefix(prefix, index);
		strings.put(prefixExec, initializer.executionName);
		strings.put(prefix + String.valueOf(index) + "." + ALGO_NAME, initializer.algorithmName);
		saveParameters(prefixExec, initializer.iterator.getParameters());
	}

	private void saveParameters(String prefix, AlgorithmConfigurationSet parameters) {
		saveIntegers(prefix + ".integer.", parameters.getIntegers(), integers);
		saveDoubles(prefix + ".double.", parameters.getDoubles(), doubles);
		saveStrings(prefix + ".string.", parameters.getStrings(), strings);
		saveSubExecutions(prefix + ".subexec.", parameters.getSubExecutions(), strings);
	}

	private void saveIntegers(String prefix, ParameterList<Integer, MpNumberIterator<Integer>> from, Map<String, Integer> to) {
		saveNumbers(prefix, from, to);
	}

	private void saveDoubles(String prefix, ParameterList<Double, MpNumberIterator<Double>> from, Map<String, Double> to) {
		saveNumbers(prefix, from, to);
	}

	private <T> void saveNumbers(String prefix, ParameterList<T, MpNumberIterator<T>> from, Map<String, T> to) {
		final List<MpNumberIterator<T>> params = from.getParams();
		integers.put(prefix + "size", params.size());
		for (int i = 0; i < params.size(); ++i) {
			final String indexPrefix = prefix + String.valueOf(i);
			strings.put(indexPrefix + ".name", params.get(i).getName());
			to.put(indexPrefix + ".from", params.get(i).getFrom());
			to.put(indexPrefix + ".to", params.get(i).getTo());
			to.put(indexPrefix + ".step", params.get(i).getStep());
		}
	}

	private void saveStrings(String prefix, ParameterList<String, MpTextIterator<String>> from, Map<String, String> to) {
		saveText(prefix, from, to);
	}

	private void saveSubExecutions(String prefix, ParameterList<String, MpTextIterator<String>> from, Map<String, String> to) {
		saveText(prefix, from, to);
	}

	private <T> void saveText(String prefix, ParameterList<T, MpTextIterator<T>> from, Map<String, T> to) {
		final List<MpTextIterator<T>> params = from.getParams();
		integers.put(prefix + "size", params.size());
		for (int i = 0; i < params.size(); ++i) {
			final MpTextIterator<T> iter = params.get(i);
			final List<T> domen = iter.getDomen();
			final String dp = prefix + String.valueOf(i) + ".";
			strings.put(dp + "name", iter.getName());
			integers.put(dp + "domen.size", domen.size());
			for (int u = 0; u < domen.size(); ++u) {
				final String domenPrefix = dp + "domen." + String.valueOf(u);
				to.put(domenPrefix + ".value", domen.get(u));
			}
		}
	}

	public SimulatorSettingsGridList getGridList(StockStorage stockStorage) throws BadParameterException {
		this.period = loadPeriod();
		final List<GridExecutionInitializer> stockInitializers = loadInitializers(STOCK_PREFIX);
		final List<GridExecutionInitializer> eodInitializers = loadInitializers(EOD_PREFIX);
		final SimulatorSettingsGridFactory factory = new SimulatorSettingsGridFactory(stockStorage, period);
		for (GridExecutionInitializer stockInit : stockInitializers) {
			factory.addStock(stockInit);
		}
		for (GridExecutionInitializer eodInit : eodInitializers) {
			factory.addEod(eodInit);
		}
		return factory.getList();
	}

	private FromToPeriod loadPeriod() {
		return new FromToPeriod(new Date(longs.get(PERIOD_FROM)), new Date(longs.get(PERIOD_TO)));
	}

	private List<GridExecutionInitializer> loadInitializers(final String prefix) throws BadParameterException {
		final int size = integers.get(prefix + INITIALIZERS_SIZE);
		final List<GridExecutionInitializer> result = new ArrayList<>(size);
		for (int i = 0; i < size; ++i) {
			final GridExecutionInitializer initializer = loadInitializer(prefix, i);
			result.add(initializer);
		}
		return result;
	}

	private GridExecutionInitializer loadInitializer(String prefix, int index) throws BadParameterException {
		final String prefixExec = createExecPrefix(prefix, index);
		final String execName = strings.get(prefixExec);
		final String algoName = strings.get(prefix + String.valueOf(index) + "." + ALGO_NAME);
		final AlgorithmConfigurationSet parameters = loadParameters(prefixExec);
		AlgorithmConfigurationSetGridGenerator paramsElement = new AlgorithmConfigurationSetGridGenerator(parameters);
		return new GridExecutionInitializer(execName, algoName, paramsElement);
	}

	private AlgorithmConfigurationSet loadParameters(String prefixExec) throws BadParameterException {
		final AlgorithmConfigurationSetImpl result = new AlgorithmConfigurationSetImpl();
		loadIntegers(result, prefixExec + ".integer.");
		loadDoubles(result, prefixExec + ".double.");
		loadStrings(result, prefixExec + ".string.");
		loadSubExecutions(result, prefixExec + ".subexec.");
		return result;
	}

	private void loadIntegers(AlgorithmConfigurationSetImpl result, String prefix) throws BadParameterException {
		final int size = integers.get(prefix + "size");
		for (int i = 0; i < size; ++i) {
			final String indexPrefix = prefix + String.valueOf(i);
			String name = strings.get(indexPrefix + ".name");
			Integer from = integers.get(indexPrefix + ".from");
			Integer to = integers.get(indexPrefix + ".to");
			Integer step = integers.get(indexPrefix + ".step");
			result.getIntegers().add(new MpInteger(name, from, to, step));
		}
	}

	private void loadDoubles(AlgorithmConfigurationSetImpl result, String prefix) throws BadParameterException {
		final int size = integers.get(prefix + "size");
		for (int i = 0; i < size; ++i) {
			final String indexPrefix = prefix + String.valueOf(i);
			String name = strings.get(indexPrefix + ".name");
			Double from = doubles.get(indexPrefix + ".from");
			Double to = doubles.get(indexPrefix + ".to");
			Double step = doubles.get(indexPrefix + ".step");
			result.getDoubles().add(new MpDouble(name, from, to, step));
		}
	}

	private void loadStrings(AlgorithmConfigurationSetImpl result, String prefix) throws BadParameterException {
		final int size = integers.get(prefix + "size");
		for (int i = 0; i < size; ++i) {
			final String dp = prefix + String.valueOf(i) + ".";
			final String name = strings.get(dp + "name");
			final int domenSize = integers.get(dp + "domen.size");
			final List<String> domen = new ArrayList<>(domenSize);
			for (int u = 0; u < domenSize; ++u) {
				final String domenPrefix = dp + "domen." + String.valueOf(u);
				domen.add(strings.get(domenPrefix + ".value"));
			}
			result.getStrings().add(new MpString(name, domen));
		}
	}

	private void loadSubExecutions(AlgorithmConfigurationSetImpl result, String prefix) throws BadParameterException {
		final int size = integers.get(prefix + "size");
		for (int i = 0; i < size; ++i) {
			final String dp = prefix + String.valueOf(i) + ".";
			final String name = strings.get(dp + "name");
			final int domenSize = integers.get(dp + "domen.size");
			final List<String> domen = new ArrayList<>(domenSize);
			for (int u = 0; u < domenSize; ++u) {
				final String domenPrefix = dp + "domen." + String.valueOf(u);
				domen.add(strings.get(domenPrefix + ".value"));
			}
			result.getSubExecutions().add(new MpSubExecution(name, domen));
		}
	}

	private String createExecPrefix(String prefix, int index) {
		return prefix + String.valueOf(index) + "." + EXEC_NAME;
	}
}
