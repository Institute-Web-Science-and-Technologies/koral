package de.uni_koblenz.west.cidre.common.executor;

import java.io.Closeable;
import java.util.Set;
import java.util.logging.Logger;

import de.uni_koblenz.west.cidre.common.query.MappingRecycleCache;

public interface WorkerTask extends Closeable {

	public boolean setUp(MappingRecycleCache recycleCache, Logger logger);

	/**
	 * The ids of two {@link WorkerTask}s should only be equal, if
	 * task1.equals(task2)==true
	 * 
	 * @return
	 */
	public int getID();

	public long getEstimatedTaskLoad();

	public long getCurrentTaskLoad();

	public Set<WorkerTask> gerPrecedingTasks();

	public boolean hasInput();

	public void execute();

	public boolean hasFinished();

	@Override
	public void close();

}
