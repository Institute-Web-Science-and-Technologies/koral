package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class StorageLog {

	private static StorageLog storageLog;

	private final String storagePath;

	private OutputStream out;

	private boolean closed;

	private StorageLog(String storagePath) {
		this.storagePath = storagePath;
	}

	public static StorageLog createInstance(String storagePath) {
		storageLog = new StorageLog(storagePath);
		return storageLog;
	}

	public static StorageLog getInstance() {
		return storageLog;
	}

	public void open() {
		try {
			out = new BufferedOutputStream(new FileOutputStream(storagePath + "storagelog"));
			closed = false;
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	public void log(long fileId, boolean write, boolean fileStorage, long cacheUsage, boolean cacheHit) {
		byte[] row = compact(fileId, write, fileStorage, cacheUsage, cacheHit);
		write(row);
	}

	private byte[] compact(long fileId, boolean write, boolean fileStorage, long cacheUsage, boolean cacheHit) {
		int bytesForCacheData = Integer.BYTES;
		byte[] row = new byte[2 + bytesForCacheData];
		// One byte is used for the fileId
		assert fileId < 128;
		row[0] = (byte) fileId;
		for (int i = 0; i < bytesForCacheData; i++) {
			row[1 + i] = (byte) (cacheUsage >> (8 * (bytesForCacheData - i)));
		}
		// Store boolean as bits in one byte
		row[1 + bytesForCacheData] = (byte) ((write ? 1 : 0) | (fileStorage ? 1 << 1 : 0) | (cacheHit ? 1 << 2 : 0));
		return row;
	}

	private void write(byte[] row) {
		if (closed) {
			open();
		}
		try {
			out.write(row);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void close() {
		if (!closed) {
			try {
				out.flush();
				out.close();
				closed = true;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
