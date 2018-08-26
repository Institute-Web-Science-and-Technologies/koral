package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log;

// Note that the SwitchToFile event is not explicitly logged, because it can be inferred from a change in the implementation flag of succeeding accesses.
enum StorageLogEvent {
	READWRITE, BLOCKFLUSH
}
