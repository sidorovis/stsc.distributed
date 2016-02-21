package stsc.distributed.common.types;

import java.io.Externalizable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import stsc.general.statistic.EquityCurve;
import stsc.general.statistic.MetricType;
import stsc.general.statistic.Metrics;

/**
 * This is implementation for {@link Externalizable} of {@link Metrics}.
 */
public final class MetricsExternalizable extends MapEasyExternalizable {

	private static String DOUBLE_SIZE = "sizeDouble";
	private static String INTEGER_SIZE = "sizeInteger";
	private static String EQUITY_VALUE_SIZE = "sizeEquityValue";

	public MetricsExternalizable(final Metrics metrics) {
		saveStatistics(metrics);
		saveEquityCurve(metrics);
	}

	public MetricsExternalizable() {
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

	private void saveEquityCurve(Metrics metrics) {
		integers.put(EQUITY_VALUE_SIZE, metrics.getEquityCurveInMoney().size());
		for (int i = 0 ; i < metrics.getEquityCurveInMoney().size() ; ++i) {
			final String parName = generateEquityCurveParameterName(i);
			longs.put(parName, metrics.getEquityCurveInMoney().get(i).date.getTime());
			doubles.put(parName, metrics.getEquityCurveInMoney().get(i).value);
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
		final int sizeEquityCurve = integers.get(EQUITY_VALUE_SIZE);
		final EquityCurve equityCurve = new EquityCurve();
		for (int i = 0 ; i < sizeEquityCurve ; ++i) {
			final String parName = generateEquityCurveParameterName(i);
			final Date date = new Date(longs.get(parName));
			final double value = doubles.get(parName);
			equityCurve.add(date, value);
		}
		return new Metrics(doubleValues, integerValues, equityCurve);
	}

	private String generateDoubleParameterName(final int index) {
		return "methodDouble." + String.valueOf(index);
	}

	private String generateIntegerParameterName(final int index) {
		return "methodInteger." + String.valueOf(index);
	}
	

	private String generateEquityCurveParameterName(final int index) {
		return "equityCurve." + String.valueOf(index);
	}
}
