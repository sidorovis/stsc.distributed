package stsc.distributed.hadoop.types;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.WritableComparable;

import stsc.algorithms.AlgorithmSettingsImpl;
import stsc.common.FromToPeriod;
import stsc.common.algorithms.AlgorithmSettings;
import stsc.common.algorithms.BadAlgorithmException;
import stsc.common.algorithms.EodExecution;
import stsc.common.algorithms.StockExecution;
import stsc.common.storage.StockStorage;
import stsc.general.simulator.SimulatorSettings;
import stsc.general.trading.TradeProcessorInit;
import stsc.storage.ExecutionsStorage;

public class SimulatorSettingsWritable extends MapEasyWritable implements WritableComparable<SimulatorSettingsWritable> {

	private static final String SIMULATOR_SETTINGS_ID = "_SimulatorSettingsId";

	private static final String PERIOD_FROM = "periodFrom";
	private static final String PERIOD_TO = "periodTo";

	private static final String STOCK_EXECUTION_SIZE = "StockExecutionsSize";
	private static final String STOCK_EXECUTIONS_PREFIX = "StockExecutions_";

	private static final String EOD_EXECUTION_SIZE = "EodExecutionsSize";
	private static final String EOD_EXECUTIONS_PREFIX = "EodExecutions_";

	private static final String STOCK_EXECUTION_NAME = ".stockExecutionName";
	private static final String STOCK_ALGORITHM_NAME = ".stockAlgorithmName";

	private static final String EOD_EXECUTION_NAME = ".eodExecutionName";
	private static final String EOD_ALGORITHM_NAME = ".eodAlgorithmName";

	private static final String INTEGERS_SIZE = ".integersSize";
	private static final String INTEGER_NAME = ".integerParameter_";

	private static final String DOUBLES_SIZE = ".doublesSize";
	private static final String DOUBLE_NAME = ".doubleParameter_";

	private static final String STRINGS_SIZE = ".stringsSize";
	private static final String STRING_NAME = ".stringParameter_";

	private static final String SUB_EXECUTIONS_SIZE = ".subExecutionsSize";
	private static final String SUB_EXECUTION_NAME = ".subExecutionName_";

	private static final String KEY_POSTFIX = ".key";
	private static final String VALUE_POSTFIX = ".value";

	// will be filled in the middle of generating
	private long id;
	private FromToPeriod period;

	protected SimulatorSettingsWritable() {
	}

	// SimulatorSettings -> SimulatorSettingsWritable
	public SimulatorSettingsWritable(final SimulatorSettings ss) {
		this();
		id = ss.getId();
		longs.put(SIMULATOR_SETTINGS_ID, ss.getId());
		saveTradeProcessorInit(ss.getInit());
	}

	// SimulatorSettings -> SimulatorSettingsWritable
	private void saveTradeProcessorInit(TradeProcessorInit init) {
		longs.put(PERIOD_FROM, init.getPeriod().getFrom().getTime());
		longs.put(PERIOD_TO, init.getPeriod().getTo().getTime());
		saveExecutionsStorage(init.getExecutionsStorage());
	}

	// SimulatorSettings -> SimulatorSettingsWritable
	private void saveExecutionsStorage(ExecutionsStorage executionsStorage) {
		final List<StockExecution> stockExecutions = executionsStorage.getStockExecutions();
		integers.put(STOCK_EXECUTION_SIZE, stockExecutions.size());
		long stockIndex = 0;
		for (StockExecution stockExecution : stockExecutions) {
			saveStockExecution(stockExecution, stockIndex);
			stockIndex += 1;
		}
		final List<EodExecution> eodExecutions = executionsStorage.getEodExecutions();
		integers.put(EOD_EXECUTION_SIZE, eodExecutions.size());
		long eodIndex = 0;
		for (EodExecution eodExecution : eodExecutions) {
			saveEodExecution(eodExecution, eodIndex);
			eodIndex += 1;
		}
	}

	// SimulatorSettings -> SimulatorSettingsWritable
	private void saveStockExecution(StockExecution stockExecution, long stockIndex) {
		final String prefix = generateStockPrefix(stockIndex);
		final String executionName = stockExecution.getExecutionName();
		strings.put(prefix + STOCK_EXECUTION_NAME, executionName);
		strings.put(prefix + STOCK_ALGORITHM_NAME, stockExecution.getAlgorithmName());
		saveAlgorithmSettings(prefix, executionName, stockExecution.getSettings());
	}

	// SimulatorSettings -> SimulatorSettingsWritable
	private void saveEodExecution(EodExecution eodExecution, long stockIndex) {
		final String prefix = generateEodPrefix(stockIndex);
		final String executionName = eodExecution.getExecutionName();
		strings.put(prefix + EOD_EXECUTION_NAME, executionName);
		strings.put(prefix + EOD_ALGORITHM_NAME, eodExecution.getAlgorithmName());
		saveAlgorithmSettings(prefix, executionName, eodExecution.getSettings());
	}

	// SimulatorSettings -> SimulatorSettingsWritable
	private void saveAlgorithmSettings(String prefix, String executionName, AlgorithmSettings settings) {
		final String algoSettingsPrefix = generateAlgoSettingsPrefix(executionName, prefix);
		saveIntegers(settings, algoSettingsPrefix);
		saveDoubles(settings, algoSettingsPrefix);
		saveStrings(settings, algoSettingsPrefix);
		saveSubExecutions(settings, algoSettingsPrefix);
	}

	private void saveIntegers(AlgorithmSettings settings, String algoSettingsPrefix) {
		saveTypes(settings, algoSettingsPrefix, settings.getIntegers(), INTEGERS_SIZE, INTEGER_NAME, integers);
	}

	private void saveDoubles(AlgorithmSettings settings, String algoSettingsPrefix) {
		saveTypes(settings, algoSettingsPrefix, settings.getDoubles(), DOUBLES_SIZE, DOUBLE_NAME, doubles);
	}

	private void saveStrings(AlgorithmSettings settings, String algoSettingsPrefix) {
		saveTypes(settings, algoSettingsPrefix, settings.getStrings(), STRINGS_SIZE, STRING_NAME, strings);
	}

	private void saveSubExecutions(AlgorithmSettings settings, String algoSettingsPrefix) {
		final List<String> originalSubExecutions = settings.getSubExecutions();
		integers.put(algoSettingsPrefix + SUB_EXECUTIONS_SIZE, originalSubExecutions.size());
		long index = 0;
		for (String i : originalSubExecutions) {
			final String parameterPrefix = algoSettingsPrefix + SUB_EXECUTION_NAME + String.valueOf(index);
			final String value = parameterPrefix + VALUE_POSTFIX;
			strings.put(value, i);
			index += 1;
		}
	}

	private <T> void saveTypes(AlgorithmSettings settings, String algoSettingsPrefix, Map<String, T> from, String sizePostfix, String fieldNamePostFix,
			Map<String, T> to) {
		integers.put(algoSettingsPrefix + sizePostfix, from.size());
		long index = 0;
		for (Entry<String, T> i : from.entrySet()) {
			final String parameterPrefix = algoSettingsPrefix + fieldNamePostFix + String.valueOf(index);
			final String key = parameterPrefix + KEY_POSTFIX;
			final String value = parameterPrefix + VALUE_POSTFIX;
			strings.put(key, i.getKey());
			to.put(value, i.getValue());
			index += 1;
		}
	}

	// SimulatorSettingsWritable -> SimulatorSettings
	public SimulatorSettings getSimulatorSettings(final StockStorage stockStorage) throws BadAlgorithmException {
		final TradeProcessorInit init = loadTradeProcessor(stockStorage);
		final long id = longs.get(SIMULATOR_SETTINGS_ID);
		this.id = id;
		return new SimulatorSettings(id, init);
	}

	// SimulatorSettingsWritable -> SimulatorSettings
	private TradeProcessorInit loadTradeProcessor(StockStorage stockStorage) throws BadAlgorithmException {
		final long periodFrom = longs.get(PERIOD_FROM);
		final long periodTo = longs.get(PERIOD_TO);
		this.period = new FromToPeriod(new Date(periodFrom), new Date(periodTo));
		final TradeProcessorInit init = new TradeProcessorInit(stockStorage, period);
		loadExecutionStorage(init.getExecutionsStorage());
		return init;
	}

	// SimulatorSettingsWritable -> SimulatorSettings
	private void loadExecutionStorage(ExecutionsStorage executionsStorage) throws BadAlgorithmException {
		final long stockExecutionsSize = integers.get(STOCK_EXECUTION_SIZE);
		for (long i = 0; i < stockExecutionsSize; ++i) {
			executionsStorage.addStockExecution(loadStockExecution(i));
		}
		final long eodExecutionsSize = integers.get(EOD_EXECUTION_SIZE);
		for (long i = 0; i < eodExecutionsSize; ++i) {
			executionsStorage.addEodExecution(loadEodExecution(i));
		}
	}

	// SimulatorSettingsWritable -> SimulatorSettings
	private StockExecution loadStockExecution(long index) throws BadAlgorithmException {
		// prefix = "StockExecutions_1"
		// prefix = "StockExecutions_24"
		// prefix = "StockExecutions_523"
		final String prefix = generateStockPrefix(index);
		final String executionName = strings.get(prefix + STOCK_EXECUTION_NAME);
		final String algorithmName = strings.get(prefix + STOCK_ALGORITHM_NAME);

		final AlgorithmSettingsImpl algorithmSettings = loadAlgorithmSettings(executionName, prefix);
		return new StockExecution(executionName, algorithmName, algorithmSettings);
	}

	// SimulatorSettingsWritable -> SimulatorSettings
	private EodExecution loadEodExecution(long index) throws BadAlgorithmException {
		// prefix = "EodExecutions_1"
		// prefix = "EodExecutions_24"
		// prefix = "EodExecutions_523"
		final String prefix = generateEodPrefix(index);
		final String executionName = strings.get(prefix + EOD_EXECUTION_NAME);
		final String algorithmName = strings.get(prefix + EOD_ALGORITHM_NAME);

		final AlgorithmSettingsImpl algorithmSettings = loadAlgorithmSettings(executionName, prefix);
		return new EodExecution(executionName, algorithmName, algorithmSettings);
	}

	// SimulatorSettingsWritable -> SimulatorSettings
	private AlgorithmSettingsImpl loadAlgorithmSettings(String executionName, String prefix) {
		final AlgorithmSettingsImpl algorithmSettings = new AlgorithmSettingsImpl(this.period);
		// algoSettingsPrefix = "StockExecutions_54.ExecutionName";
		// algoSettingsPrefix = "EodExecutions_76.TheUserDefinedName";
		final String algoSettingsPrefix = generateAlgoSettingsPrefix(executionName, prefix);
		loadIntegers(algorithmSettings, algoSettingsPrefix);
		loadDoubles(algorithmSettings, algoSettingsPrefix);
		loadStrings(algorithmSettings, algoSettingsPrefix);
		loadSubExecutions(algorithmSettings, algoSettingsPrefix);
		return algorithmSettings;
	}

	private void loadIntegers(AlgorithmSettingsImpl algorithmSettings, String algoSettingsPrefix) {
		final long algoSettingsSize = integers.get(algoSettingsPrefix + INTEGERS_SIZE);
		for (long i = 0; i < algoSettingsSize; ++i) {
			final String parameterPrefix = algoSettingsPrefix + INTEGER_NAME + String.valueOf(i);
			final String key = parameterPrefix + KEY_POSTFIX;
			final String value = parameterPrefix + VALUE_POSTFIX;
			algorithmSettings.setInteger(strings.get(key), integers.get(value));
		}
	}

	private void loadDoubles(AlgorithmSettingsImpl algorithmSettings, String algoSettingsPrefix) {
		final long algoSettingsSize = integers.get(algoSettingsPrefix + DOUBLES_SIZE);
		for (long i = 0; i < algoSettingsSize; ++i) {
			final String parameterPrefix = algoSettingsPrefix + DOUBLE_NAME + String.valueOf(i);
			final String key = parameterPrefix + KEY_POSTFIX;
			final String value = parameterPrefix + VALUE_POSTFIX;
			algorithmSettings.setDouble(strings.get(key), doubles.get(value));
		}
	}

	private void loadStrings(AlgorithmSettingsImpl algorithmSettings, String algoSettingsPrefix) {
		final long algoSettingsSize = integers.get(algoSettingsPrefix + STRINGS_SIZE);
		for (long i = 0; i < algoSettingsSize; ++i) {
			final String parameterPrefix = algoSettingsPrefix + STRING_NAME + String.valueOf(i);
			final String key = parameterPrefix + KEY_POSTFIX;
			final String value = parameterPrefix + VALUE_POSTFIX;
			algorithmSettings.setString(strings.get(key), strings.get(value));
		}
	}

	private void loadSubExecutions(AlgorithmSettingsImpl algorithmSettings, String algoSettingsPrefix) {
		final long algoSettingsSize = integers.get(algoSettingsPrefix + SUB_EXECUTIONS_SIZE);
		for (long i = 0; i < algoSettingsSize; ++i) {
			final String parameterPrefix = algoSettingsPrefix + SUB_EXECUTION_NAME + String.valueOf(i);
			final String value = parameterPrefix + VALUE_POSTFIX;
			algorithmSettings.addSubExecutionName(strings.get(value));
		}
	}

	// common methods for SimulatorSettingsWritable -> SimulatorSettings
	// and SimulatorSettings -> SimulatorSettingsWritable
	private String generateStockPrefix(long index) {
		return STOCK_EXECUTIONS_PREFIX + String.valueOf(index);
	}

	// common methods for SimulatorSettingsWritable -> SimulatorSettings
	// and SimulatorSettings -> SimulatorSettingsWritable
	private String generateEodPrefix(long index) {
		return EOD_EXECUTIONS_PREFIX + String.valueOf(index);
	}

	private String generateAlgoSettingsPrefix(String executionName, String prefix) {
		return prefix + "." + executionName;
	}

	@Override
	public int compareTo(SimulatorSettingsWritable o) {
		return (int) (this.id - o.id);
	}
}
