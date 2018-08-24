package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.Utils;

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
			out = new BufferedOutputStream(new GZIPOutputStream(new FileOutputStream(storagePath + "storagelog")));
			closed = false;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void logAcessEvent(long fileId, int blockId, boolean write, boolean fileStorage, long cacheUsage,
			boolean cacheHit) {
		int cursor = 0;
		int bytesForCacheData = Integer.BYTES;
		byte[] row = new byte[2 + Integer.BYTES + bytesForCacheData + 1];

		row[cursor] = (byte) StorageLogEvent.READWRITE.ordinal();
		cursor += 1;

		// One byte is used for the fileId
		// TODO: Infer bytes needed
		if (fileId > 127) {
			throw new IllegalArgumentException("FileId is too large. Compaction strategy must be adjusted");
		}
		row[cursor] = (byte) fileId;
		cursor += 1;

		Utils.writeLongIntoBytes(blockId, row, cursor, Integer.BYTES);
		cursor += Integer.BYTES;

		Utils.writeLongIntoBytes(cacheUsage, row, cursor, bytesForCacheData);
		cursor += bytesForCacheData;

		// Store boolean as bits in one byte
		row[cursor] = (byte) ((write ? 1 : 0) | (fileStorage ? 1 << 1 : 0) | (cacheHit ? 1 << 2 : 0));

		write(row);
	}

	public void logBlockFlushEvent(long fileId, int blockId, boolean dirty) {
		int cursor = 0;
		byte[] row = new byte[2 + Integer.BYTES + 1];

		row[cursor] = (byte) StorageLogEvent.BLOCKFLUSH.ordinal();
		cursor += 1;

		// One byte is used for the fileId
		// TODO: Infer bytes needed
		if (fileId > 127) {
			throw new IllegalArgumentException("FileId is too large. Compaction strategy must be adjusted");
		}
		row[cursor] = (byte) fileId;
		cursor += 1;

		Utils.writeLongIntoBytes(blockId, row, cursor, Integer.BYTES);
		cursor += Integer.BYTES;

		row[cursor] = (byte) (dirty ? 1 : 0);

		write(row);
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
