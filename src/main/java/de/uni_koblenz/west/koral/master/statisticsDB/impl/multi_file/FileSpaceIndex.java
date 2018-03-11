package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

import de.uni_koblenz.west.koral.common.utils.ReusableIDGenerator;

public class FileSpaceIndex {

	private final String storagePath;

	private final TreeMap<Long, ReusableIDGenerator> indexes;

	private final File storage;

	public FileSpaceIndex(String storagePath) {
		this.storagePath = storagePath;

		indexes = new TreeMap<>();
		storage = new File(storagePath + "fileSpaceIndexes");
	}

	long getFreeRow(long fileId) {
		ReusableIDGenerator fileSpaceIndex = indexes.get(fileId);
		if (fileSpaceIndex == null) {
			fileSpaceIndex = new ReusableIDGenerator();
			indexes.put(fileId, fileSpaceIndex);
		}
		return fileSpaceIndex.getNextId();
	}

	void release(long fileId, long rowId) {
		indexes.get(fileId).release(rowId);
	}

	List<Long> getEmptyFiles() {
		LinkedList<Long> emptyFiles = new LinkedList<>();
		for (ReusableIDGenerator rig : indexes.values()) {
			// TODO: Extend ReusableIDGenerator with getData() or isEmpty() method
		}
		return emptyFiles;
	}

	void flush() {
		// TODO: Extend ReusableIDGenerator with getData() method
	}

	void clear() {
		indexes.clear();
		storage.delete();
	}

	void close() {
		flush();
	}
}
