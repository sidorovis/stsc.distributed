package stsc.distributed.hadoop.grid;

import java.util.List;

import stsc.general.strategy.TradingStrategy;

/**
 * Simple interface for Hadoop starter. <br/>
 */
public interface HadoopStarter {

	List<TradingStrategy> searchOnHadoop() throws Exception;

}
