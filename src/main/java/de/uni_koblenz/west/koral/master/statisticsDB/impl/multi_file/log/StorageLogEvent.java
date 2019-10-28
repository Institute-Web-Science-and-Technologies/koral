package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log;

/**
 * Enumerates the different considered events which the {@link StorageLogWriter} listens to.
 *
 * @author Philipp Töws
 *
 */
// Note that the SwitchToFile event is not explicitly logged, because it can be inferred from a change in the implementation flag of succeeding accesses.
public enum StorageLogEvent {
	READWRITE,
	BLOCKFLUSH,
	CHUNKSWITCH
}
