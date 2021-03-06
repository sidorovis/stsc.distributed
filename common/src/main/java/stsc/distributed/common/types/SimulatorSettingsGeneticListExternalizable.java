package stsc.distributed.common.types;

import java.io.Externalizable;
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
import stsc.general.simulator.multistarter.genetic.AlgorithmConfigurationSetGeneticGenerator;
import stsc.general.simulator.multistarter.genetic.ExternalizableGeneticList;
import stsc.general.simulator.multistarter.genetic.GeneticExecutionInitializer;
import stsc.general.simulator.multistarter.genetic.SimulatorSettingsGeneticFactory;
import stsc.general.simulator.multistarter.genetic.SimulatorSettingsGeneticListImpl;

/**
 * This is implementation for {@link Externalizable} of {@link SimulatorSettingsGeneticListImpl}.
 */
public final class SimulatorSettingsGeneticListExternalizable extends MapEasyExternalizable {

	private static final String GENETIC_LIST_ID = "_GeneticList_Id";

	private static final String PERIOD_FROM = "periodFrom";
	private static final String PERIOD_TO = "periodTo";

	private static final String STOCK_PREFIX = "stockInit.";
	private static final String EOD_PREFIX = "eodInit.";

	private static final String EXECUTION_NAME = "execName";
	private static final String ALGORITHM_NAME = "algoName";

	private static final String INTEGERS_SIZE = "integerSize";
	private static final String DOUBLES_SIZE = "doubleSize";
	private static final String STRINGS_SIZE = "stringSize";
	private static final String SUB_EXECUTIONS_SIZE = "subExecSize";

	private static final String INTEGER_NAME = "integerName";
	private static final String DOUBLE_NAME = "doubleName";
	private static final String STRING_NAME = "stringName";
	private static final String SUB_EXECUTION_NAME = "subExecName";

	// will be filled in the middle of generating
	private FromToPeriod period;

	// for reading
	public SimulatorSettingsGeneticListExternalizable() {
	}

	// List -> Writable
	public SimulatorSettingsGeneticListExternalizable(final ExternalizableGeneticList list) {
		this();
		saveGeneticList(list);
	}

	private void saveGeneticList(ExternalizableGeneticList list) {
		longs.put(GENETIC_LIST_ID, list.getId());
		longs.put(PERIOD_FROM, list.getPeriod().getFrom().getTime());
		longs.put(PERIOD_TO, list.getPeriod().getTo().getTime());
		saveStocks(list);
		saveEods(list);
	}

	private void saveStocks(ExternalizableGeneticList list) {
		saveInitializers(STOCK_PREFIX, list.getStockInitializers());
	}

	private void saveEods(ExternalizableGeneticList list) {
		saveInitializers(EOD_PREFIX, list.getEodInitializers());
	}

	private void saveInitializers(String prefix, List<GeneticExecutionInitializer> stockInitializers) {
		long index = 0;
		integers.put(prefix + "size", stockInitializers.size());
		for (GeneticExecutionInitializer initializer : stockInitializers) {
			saveInitializer(prefix, index, initializer);
			index += 1;
		}
	}

	private void saveInitializer(String prefix, long index, GeneticExecutionInitializer initializer) {
		final String initPrefix = prefix + String.valueOf(index) + ".";
		final String execPrefix = initPrefix + EXECUTION_NAME + ".";
		strings.put(execPrefix, initializer.getExecutionName());
		strings.put(initPrefix + ALGORITHM_NAME, initializer.getAlgorithmName());
		saveAlgorithmSettings(execPrefix, initializer.geneticAlgorithmSettings);
	}

	private void saveAlgorithmSettings(String execPrefix, AlgorithmConfigurationSetGeneticGenerator geneticGenerator) {
		saveParameters(execPrefix, geneticGenerator.getParameters());

	}

	private void saveParameters(String execPrefix, AlgorithmConfigurationSet parameters) {
		saveIntegers(execPrefix, parameters.getIntegers());
		saveDoubles(execPrefix, parameters.getDoubles());
		saveStrings(execPrefix, parameters.getStrings());
		saveSubExecutions(execPrefix, parameters.getSubExecutions());
	}

	private void saveIntegers(String execPrefix, ParameterList<Integer, MpNumberIterator<Integer>> params) {
		saveNumberType(execPrefix, params, integers, INTEGERS_SIZE, INTEGER_NAME);
	}

	private void saveDoubles(String execPrefix, ParameterList<Double, MpNumberIterator<Double>> params) {
		saveNumberType(execPrefix, params, doubles, DOUBLES_SIZE, DOUBLE_NAME);
	}

	private void saveStrings(String execPrefix, ParameterList<String, MpTextIterator<String>> params) {
		saveTextType(execPrefix, params, strings, STRINGS_SIZE, STRING_NAME);
	}

	private void saveSubExecutions(String execPrefix, ParameterList<String, MpTextIterator<String>> params) {
		saveTextType(execPrefix, params, strings, SUB_EXECUTIONS_SIZE, SUB_EXECUTION_NAME);
	}

	private <T> void saveNumberType(String execPrefix, ParameterList<T, MpNumberIterator<T>> params, Map<String, T> to, String sizePostfix, String namePostfix) {
		final List<MpNumberIterator<T>> p = params.getParams();
		integers.put(execPrefix + sizePostfix, p.size());
		int index = 0;
		for (MpNumberIterator<T> mp : p) {
			final String prefix = execPrefix + namePostfix + String.valueOf(index);
			strings.put(prefix, mp.getName());
			to.put(prefix + ".from", mp.getFrom());
			to.put(prefix + ".to", mp.getTo());
			to.put(prefix + ".step", mp.getStep());
			index += 1;
		}
	}

	private void saveTextType(String execPrefix, ParameterList<String, MpTextIterator<String>> params, Map<String, String> to, String sizePostfix, String namePostfix) {
		final List<MpTextIterator<String>> p = params.getParams();
		integers.put(execPrefix + sizePostfix, p.size());
		int index = 0;
		for (MpTextIterator<String> mp : p) {
			final String prefix = execPrefix + namePostfix + "." + String.valueOf(index);
			strings.put(prefix, mp.getName());
			integers.put(prefix, mp.getDomen().size());

			int domenIndex = 0;
			for (String s : mp.getDomen()) {
				final String key = prefix + ".domen." + String.valueOf(domenIndex);
				strings.put(key, s);
				domenIndex += 1;
			}
			index += 1;
		}
	}

	public SimulatorSettingsGeneticListImpl getGeneticList(StockStorage stockStorage) throws BadParameterException {
		final long periodFrom = longs.get(PERIOD_FROM);
		final long periodTo = longs.get(PERIOD_TO);
		this.period = new FromToPeriod(new Date(periodFrom), new Date(periodTo));
		final List<GeneticExecutionInitializer> stockInitializers = loadStockInitializers();
		final List<GeneticExecutionInitializer> eodInitializers = loadEodInitializers();
		final SimulatorSettingsGeneticFactory factory = new SimulatorSettingsGeneticFactory(stockStorage, period);
		for (GeneticExecutionInitializer stockInit : stockInitializers) {
			factory.addStock(stockInit);
		}
		for (GeneticExecutionInitializer eodInit : eodInitializers) {
			factory.addEod(eodInit);
		}
		return factory.getList();
	}

	private List<GeneticExecutionInitializer> loadStockInitializers() throws BadParameterException {
		final List<GeneticExecutionInitializer> stockInitializers = new ArrayList<>();
		loadInitializers(stockInitializers, STOCK_PREFIX);
		return stockInitializers;
	}

	private List<GeneticExecutionInitializer> loadEodInitializers() throws BadParameterException {
		final List<GeneticExecutionInitializer> eodInitializers = new ArrayList<>();
		loadInitializers(eodInitializers, EOD_PREFIX);
		return eodInitializers;
	}

	private void loadInitializers(List<GeneticExecutionInitializer> initializers, String prefix) throws BadParameterException {
		final int size = integers.get(prefix + "size");
		for (int i = 0; i < size; ++i) {
			final GeneticExecutionInitializer initializer = loadInitializer(prefix, i);
			initializers.add(initializer);
		}
	}

	private GeneticExecutionInitializer loadInitializer(String prefix, int index) throws BadParameterException {
		final String initPrefix = prefix + String.valueOf(index) + ".";
		final String execPrefix = initPrefix + EXECUTION_NAME + ".";
		final String executionName = strings.get(execPrefix);
		final String algorithmName = strings.get(initPrefix + ALGORITHM_NAME);
		final AlgorithmConfigurationSetGeneticGenerator list = loadAlgorithmSettings(execPrefix);
		return new GeneticExecutionInitializer(executionName, algorithmName, list);
	}

	private AlgorithmConfigurationSetGeneticGenerator loadAlgorithmSettings(String execPrefix) throws BadParameterException {
		return new AlgorithmConfigurationSetGeneticGenerator(loadAlgorithmParameters(execPrefix));
	}

	private AlgorithmConfigurationSetImpl loadAlgorithmParameters(String execPrefix) throws BadParameterException {
		final AlgorithmConfigurationSetImpl result = new AlgorithmConfigurationSetImpl();
		loadIntegers(result, execPrefix);
		loadDoubles(result, execPrefix);
		loadStrings(result, execPrefix);
		loadSubExecutions(result, execPrefix);
		return result;
	}

	private void loadIntegers(AlgorithmConfigurationSetImpl result, String execPrefix) throws BadParameterException {
		loadIntegerType(execPrefix, result, integers, INTEGERS_SIZE, INTEGER_NAME);
	}

	private void loadDoubles(AlgorithmConfigurationSetImpl result, String execPrefix) throws BadParameterException {
		loadDoubleType(execPrefix, result, doubles, DOUBLES_SIZE, DOUBLE_NAME);
	}

	private void loadStrings(AlgorithmConfigurationSetImpl result, String execPrefix) throws BadParameterException {
		final int size = integers.get(execPrefix + STRINGS_SIZE);
		for (int index = 0; index < size; ++index) {
			final String prefix = execPrefix + STRING_NAME + "." + String.valueOf(index);

			final String name = strings.get(prefix);
			final Integer domenSize = integers.get(prefix);

			final List<String> domen = new ArrayList<>(domenSize);

			for (int domenIndex = 0; domenIndex < domenSize; domenIndex++) {
				final String key = prefix + ".domen." + String.valueOf(domenIndex);
				domen.add(strings.get(key));
			}
			result.getStrings().add(new MpString(name, domen));
		}
	}

	private void loadSubExecutions(AlgorithmConfigurationSetImpl result, String execPrefix) throws BadParameterException {
		final int size = integers.get(execPrefix + SUB_EXECUTIONS_SIZE);
		for (int index = 0; index < size; ++index) {
			final String prefix = execPrefix + SUB_EXECUTION_NAME + "." + String.valueOf(index);

			final String name = strings.get(prefix);
			final Integer domenSize = integers.get(prefix);

			final List<String> domen = new ArrayList<>(domenSize);

			for (int domenIndex = 0; domenIndex < domenSize; domenIndex++) {
				final String key = prefix + ".domen." + String.valueOf(domenIndex);
				domen.add(strings.get(key));
			}
			result.getSubExecutions().add(new MpSubExecution(name, domen));
		}
	}

	private void loadIntegerType(String execPrefix, AlgorithmConfigurationSetImpl result, Map<String, Integer> params, String sizePostfix, String namePostfix)
			throws BadParameterException {
		final int size = integers.get(execPrefix + sizePostfix);
		for (int index = 0; index < size; ++index) {
			final String prefix = execPrefix + namePostfix + String.valueOf(index);

			final String name = strings.get(prefix);
			Integer from = params.get(prefix + ".from");
			Integer to = params.get(prefix + ".to");
			Integer step = params.get(prefix + ".step");

			result.getIntegers().add(new MpInteger(name, from, to, step));
		}
	}

	private void loadDoubleType(String execPrefix, AlgorithmConfigurationSetImpl result, Map<String, Double> params, String sizePostfix, String namePostfix)
			throws BadParameterException {
		final int size = integers.get(execPrefix + sizePostfix);
		for (int index = 0; index < size; ++index) {
			final String prefix = execPrefix + namePostfix + String.valueOf(index);

			final String name = strings.get(prefix);
			Double from = params.get(prefix + ".from");
			Double to = params.get(prefix + ".to");
			Double step = params.get(prefix + ".step");

			result.getDoubles().add(new MpDouble(name, from, to, step));
		}
	}
}