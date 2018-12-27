package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

import java.io.IOException;
import java.util.logging.Logger;

import de.uni_koblenz.west.koral.common.utils.ReusableIDGenerator;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.shared_space.HABSESharedSpaceManager;

public class ExtraStorageAccessor extends StorageAccessor implements ExtraRowStorage {

	private final ReusableIDGenerator freeSpaceIndex;

	public ExtraStorageAccessor(String storageFilePath, long fileId, int rowLength,
			HABSESharedSpaceManager extraCacheSpaceManager, long[] loadedFreeSpaceIndex, boolean createIfNotExisting,
			Logger logger) {
		super(storageFilePath, fileId, rowLength, extraCacheSpaceManager, true, createIfNotExisting, logger);
		freeSpaceIndex = new ReusableIDGenerator(loadedFreeSpaceIndex);
	}

	@Override
	public byte[] readRow(long rowId) throws IOException {
//		long start = System.nanoTime();
		if (!freeSpaceIndex.isUsed(rowId)) {
			return null;
		}
//		CentralLogger.getInstance().addTime("RLE_IS_USED", System.nanoTime() - start);
		return super.readRow(rowId);
	}

	@Override
	public long writeRow(byte[] row) throws IOException {
//		long start = System.nanoTime();
		long rowId = freeSpaceIndex.next();
//		CentralLogger.getInstance().addTime("RLE_NEXT", System.nanoTime() - start);
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

	public long getMaxRowId() {
		return freeSpaceIndex.getMaxId();
	}

	@Override
	public long accessCosts() {
		return freeSpaceIndex.getMaxId();
	}

	@Override
	public boolean isEmpty() {
		return freeSpaceIndex.isEmpty();
	}

}
