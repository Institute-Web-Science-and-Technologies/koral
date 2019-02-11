package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

import java.io.IOException;
import java.util.logging.Logger;

import de.uni_koblenz.west.koral.common.utils.DynamicRLEList;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.SubbenchmarkManager;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.shared_space.HABSESharedSpaceManager;
import playground.StatisticsDBTest;

public class ExtraStorageAccessor extends StorageAccessor implements ExtraRowStorage {

	private final DynamicRLEList freeSpaceIndex;

	public ExtraStorageAccessor(String storageFilePath, long fileId, int rowLength,
			HABSESharedSpaceManager extraCacheSpaceManager, byte[] loadedFreeSpaceIndex, boolean createIfNotExisting,
			Logger logger) {
		super(storageFilePath, fileId, rowLength, extraCacheSpaceManager, true, createIfNotExisting, logger);
		freeSpaceIndex = new DynamicRLEList(loadedFreeSpaceIndex);
	}

	@Override
	public byte[] readRow(long rowId) throws IOException {
		return super.readRow(rowId);
	}

	@Override
	public long writeRow(byte[] row) throws IOException {
		long start = 0;
		if (StatisticsDBTest.SUBBENCHMARKS) {
			start = System.nanoTime();
		}
		long rowId = freeSpaceIndex.next();
		if (StatisticsDBTest.SUBBENCHMARKS) {
			SubbenchmarkManager.getInstance().addTime(SubbenchmarkManager.SUBBENCHMARK_TASK.RLE_NEXT,
					System.nanoTime() - start);
		}
		writeRow(rowId, row);
		return rowId;
	}

	@Override
	public void deleteRow(long rowId) {
		long start = 0;
		if (StatisticsDBTest.SUBBENCHMARKS) {
			start = System.nanoTime();
		}
		freeSpaceIndex.release(rowId);
		if (StatisticsDBTest.SUBBENCHMARKS) {
			SubbenchmarkManager.getInstance().addTime(SubbenchmarkManager.SUBBENCHMARK_TASK.RLE_RELEASE,
					System.nanoTime() - start);
		}
	}

	@Override
	public void defragFreeSpaceIndex() {
		freeSpaceIndex.defrag();
	}

	@Override
	public byte[] getFreeSpaceIndexData() {
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
