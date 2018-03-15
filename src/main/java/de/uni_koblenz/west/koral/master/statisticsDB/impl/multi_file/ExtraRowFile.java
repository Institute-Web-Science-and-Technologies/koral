package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file;

import java.io.IOException;

import de.uni_koblenz.west.koral.common.utils.ReusableIDGenerator;

class ExtraRowFile extends RowFile {

	private final ReusableIDGenerator freeSpaceIndex;

	public ExtraRowFile(String storageFilePath, boolean createIfNotExists) {
		super(storageFilePath, createIfNotExists);
		freeSpaceIndex = new ReusableIDGenerator();
	}

	public ExtraRowFile(String storageFilePath, boolean createIfNotExists, long[] list) {
		super(storageFilePath, createIfNotExists);
		freeSpaceIndex = new ReusableIDGenerator(list);
	}

	long writeRow(byte[] row) throws IOException {
		long rowId = freeSpaceIndex.getNextId();
		writeRow(rowId, row);
		return rowId;
	}

	void deleteRow(long rowId) {
		freeSpaceIndex.release(rowId);
	}

	long[] getFreeSpaceIndexData() {
		return freeSpaceIndex.getData();
	}

	boolean isEmpty() {
		return freeSpaceIndex.isEmpty();
	}

}
