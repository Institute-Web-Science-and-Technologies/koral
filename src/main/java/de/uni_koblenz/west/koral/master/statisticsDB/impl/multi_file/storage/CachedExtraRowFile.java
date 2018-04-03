package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

import java.io.IOException;

import de.uni_koblenz.west.koral.common.utils.ReusableIDGenerator;

public class CachedExtraRowFile extends CachedRowFile implements ExtraRowStorage {

	private final ReusableIDGenerator freeSpaceIndex;

	public CachedExtraRowFile(String storageFilePath, int rowLength, int maxCacheSize, boolean createIfNotExists) {
		this(storageFilePath, rowLength, maxCacheSize, createIfNotExists, null);
	}

	public CachedExtraRowFile(String storageFilePath, int rowLength, int maxCacheSize, boolean createIfNotExists,
			long[] list) {
		super(storageFilePath, rowLength, maxCacheSize, createIfNotExists);
		freeSpaceIndex = new ReusableIDGenerator(list);
	}

	@Override
	public long writeRow(byte[] row) throws IOException {
		long rowId = freeSpaceIndex.getNextId();
		writeRow(rowId, row);
		return rowId;
	}

	@Override
	public void deleteRow(long rowId) {
		freeSpaceIndex.release(rowId);
	}

	@Override
	public long[] getFreeSpaceIndexData() {
		return freeSpaceIndex.getData();
	}

	@Override
	public boolean isEmpty() {
		return freeSpaceIndex.isEmpty();
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + '@' + Integer.toHexString(hashCode());
	}

}
