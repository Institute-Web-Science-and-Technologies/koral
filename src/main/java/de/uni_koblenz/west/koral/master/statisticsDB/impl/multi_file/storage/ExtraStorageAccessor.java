package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

import java.io.IOException;
import java.util.logging.Logger;

import de.uni_koblenz.west.koral.common.utils.ReusableIDGenerator;

public class ExtraStorageAccessor extends StorageAccessor implements ExtraRowStorage {

	private final ReusableIDGenerator freeSpaceIndex;

	public ExtraStorageAccessor(String storageFilePath, int rowLength, long maxCacheSize, Logger logger) {
		this(storageFilePath, rowLength, maxCacheSize, null, logger);
	}

	public ExtraStorageAccessor(String storageFilePath, int rowLength, long maxCacheSize, long[] loadedFreeSpaceIndex,
			Logger logger) {
		super(storageFilePath, rowLength, maxCacheSize, logger);
		freeSpaceIndex = new ReusableIDGenerator(loadedFreeSpaceIndex);
	}

	@Override
	public byte[] readRow(long rowId) throws IOException {
		if (!freeSpaceIndex.isUsed(rowId)) {
			return null;
		}
		return super.readRow(rowId);
	}

	@Override
	public long writeRow(byte[] row) throws IOException {
		long rowId = freeSpaceIndex.next();
		writeRow(rowId, row);
		return rowId;
	}

	@Override
	public void deleteRow(long rowId) {
		freeSpaceIndex.release(rowId);
	}

	@Override
	public void defragFreeSpaceIndex() {
		freeSpaceIndex.defrag();
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
