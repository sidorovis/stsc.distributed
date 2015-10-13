package stsc.distributed.hadoop.grid;

import java.util.List;

import stsc.general.strategy.TradingStrategy;

/**
 * Simple interface for Hadoop starter. <br/>
 * TODO should be modified and improved.
 */
public interface HadoopStarter {

	public List<TradingStrategy> searchOnHadoop() throws Exception;

}
