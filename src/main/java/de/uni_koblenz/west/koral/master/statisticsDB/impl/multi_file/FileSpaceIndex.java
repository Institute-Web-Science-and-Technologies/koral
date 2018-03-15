package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import de.uni_koblenz.west.koral.common.io.EncodedLongFileOutputStream;
import de.uni_koblenz.west.koral.common.utils.ReusableIDGenerator;

public class FileSpaceIndex {

	private final TreeMap<Long, ReusableIDGenerator> indexes;

	private final File storage;

	public FileSpaceIndex(String storagePath) {
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
		for (Entry<Long, ReusableIDGenerator> entry : indexes.entrySet()) {
			if (entry.getValue().isEmpty()) {
				emptyFiles.add(entry.getKey());
			}
		}
		return emptyFiles;
	}

	void flush() {
		for (Entry<Long, ReusableIDGenerator> entry : indexes.entrySet()) {
			long[] data = entry.getValue().getData();
			try (FileOutputStream fileOutputStream = new FileOutputStream(storage);) {
				try (EncodedLongFileOutputStream out = new EncodedLongFileOutputStream(storage, true);) {
					
				}
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}  
		}

	}

	void clear() {
		indexes.clear();
		storage.delete();
	}

	void close() {
		flush();
	}
}
