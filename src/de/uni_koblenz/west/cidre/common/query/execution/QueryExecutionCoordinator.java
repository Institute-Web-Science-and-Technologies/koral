package de.uni_koblenz.west.cidre.common.query.execution;

import java.io.Closeable;
import java.util.Set;
import java.util.logging.Logger;

import de.uni_koblenz.west.cidre.common.executor.WorkerTask;
import de.uni_koblenz.west.cidre.common.executor.messagePassing.MessageSenderBuffer;
import de.uni_koblenz.west.cidre.common.query.MappingRecycleCache;
import de.uni_koblenz.west.cidre.master.client_manager.ClientConnectionManager;
import de.uni_koblenz.west.cidre.master.dictionary.DictionaryEncoder;
import de.uni_koblenz.west.cidre.master.statisticsDB.GraphStatistics;

/**
 * Coordinates the query execution and sends messages to the requesting client.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class QueryExecutionCoordinator implements WorkerTask, Closeable {

	private final Logger logger;

	// TODO before sending to sparql reqester replace urn:blankNode: by _: for
	// proper blank node syntax

	public QueryExecutionCoordinator(int queryID, int clientID,
			ClientConnectionManager clientConnections,
			DictionaryEncoder dictionary, GraphStatistics statistics,
			Logger logger) {
		this.logger = logger;
		// TODO Auto-generated constructor stub
	}

	@Override
	public void setUp(MessageSenderBuffer messageSender,
			MappingRecycleCache recycleCache, Logger logger) {
		// TODO Auto-generated method stub
	}

	@Override
	public long getID() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getCoordinatorID() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getEstimatedTaskLoad() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getCurrentTaskLoad() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public WorkerTask getParentTask() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<WorkerTask> getPrecedingTasks() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void start() {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean hasInput() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void enqueueMessage(long sender, byte[] message, int firstIndex) {
		// receives message of finished root tasks!!!
		// TODO Auto-generated method stub

	}

	@Override
	public void execute() {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean hasFinished() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	public int getQueryId() {
		// TODO Auto-generated method stub
		return 0;
	}

}
