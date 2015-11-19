package de.uni_koblenz.west.cidre.common.query.execution;

import java.io.File;
import java.util.logging.Logger;

import de.uni_koblenz.west.cidre.common.executor.WorkerTask;
import de.uni_koblenz.west.cidre.common.messages.MessageUtils;
import de.uni_koblenz.west.cidre.common.utils.NumberConversion;
import de.uni_koblenz.west.cidre.master.client_manager.ClientConnectionManager;
import de.uni_koblenz.west.cidre.master.dictionary.DictionaryEncoder;
import de.uni_koblenz.west.cidre.master.statisticsDB.GraphStatistics;

/**
 * Coordinates the query execution and sends messages to the requesting client.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class QueryExecutionCoordinator extends QueryTaskBase {

	// TODO before sending to sparql reqester replace urn:blankNode: by _: for
	// proper blank node syntax

	public QueryExecutionCoordinator(short computerID, int queryID,
			int numberOfSlaves, int cacheSize, File cacheDir, int clientID,
			ClientConnectionManager clientConnections,
			DictionaryEncoder dictionary, GraphStatistics statistics,
			Logger logger) {
		super(computerID, queryID, (short) 0, Integer.MAX_VALUE, numberOfSlaves,
				cacheSize, cacheDir);
		// TODO Auto-generated constructor stub
	}

	public void processQueryRequest(byte[][] arguments) {
		int queryTypeOctal = NumberConversion.bytes2int(arguments[0]);
		String queryString = MessageUtils.convertToString(arguments[1], logger);
		// TODO Auto-generated method stub

	}

	public int getQueryId() {
		return (int) ((getID() & 0x00_00_ff_ff_ff_ff_00_00l) >>> Short.SIZE);
	}

	@Override
	public long getCoordinatorID() {
		return getID();
	}

	@Override
	public long getCurrentTaskLoad() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public WorkerTask getParentTask() {
		return null;
	}

	@Override
	protected void handleMappingReception(long sender, byte[] message,
			int firstIndex) {
		// TODO Auto-generated method stub

	}

	@Override
	protected void executePreStartStep() {
		// TODO Auto-generated method stub

	}

	@Override
	protected void executeOperationStep() {
		// TODO Auto-generated method stub

	}

	@Override
	protected boolean isFinishedInternal() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void close() {
		super.close();
		// TODO Auto-generated method stub

	}

}
