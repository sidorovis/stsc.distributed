package stsc.distributed.hadoop.types;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Writable;

import stsc.general.statistic.MetricType;
import stsc.general.statistic.Metrics;

/**
 * This is implementation for {@link Writable} of {@link Metrics}.
 */
public final class MetricsWritable extends MapEasyWritable {

	private static String DOUBLE_SIZE = "sizeDouble";
	private static String INTEGER_SIZE = "sizeInteger";

	public MetricsWritable(final Metrics metrics) {
		saveStatistics(metrics);
	}

	protected MetricsWritable() {
	}

	private void saveStatistics(final Metrics metrics) {
		integers.put(DOUBLE_SIZE, metrics.getDoubleMetrics().size());
		int index = 0;
		for (Map.Entry<MetricType, Double> e : metrics.getDoubleMetrics().entrySet()) {
			final String parName = generateDoubleParameterName(index);
			strings.put(parName, e.getKey().name());
			doubles.put(parName, e.getValue());
			index += 1;
		}
		integers.put(INTEGER_SIZE, metrics.getIntegerMetrics().size());
		index = 0;
		for (Map.Entry<MetricType, Integer> e : metrics.getIntegerMetrics().entrySet()) {
			final String parName = generateIntegerParameterName(index);
			strings.put(parName, e.getKey().name());
			integers.put(parName, e.getValue());
			index += 1;
		}
	}

	public Metrics getMetrics() {
		return loadStatistics();
	}

	private Metrics loadStatistics() {
		final int sizeDouble = integers.get(DOUBLE_SIZE);
		final Map<MetricType, Double> doubleValues = new HashMap<>();
		for (int i = 0; i < sizeDouble; ++i) {
			final String parName = generateDoubleParameterName(i);
			doubleValues.put(MetricType.valueOf(strings.get(parName)), doubles.get(parName));
		}
		final int sizeInteger = integers.get(INTEGER_SIZE);
		final Map<MetricType, Integer> integerValues = new HashMap<>();
		for (int i = 0; i < sizeInteger; ++i) {
			final String parName = generateIntegerParameterName(i);
			integerValues.put(MetricType.valueOf(strings.get(parName)), integers.get(parName));
		}
		return new Metrics(doubleValues, integerValues);
	}

	private String generateDoubleParameterName(final int index) {
		return "methodDouble." + String.valueOf(index);
	}

	private String generateIntegerParameterName(final int index) {
		return "methodInteger." + String.valueOf(index);
	}
}
