package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.counter.impl;

import java.io.File;
import java.util.Iterator;

import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.counter.PersistantStore;

public class RocksDBStore implements PersistantStore {

	private RocksDB counter;

	public RocksDBStore(String storageDir) {
		this(storageDir, 400);
	}

	public RocksDBStore(String storageDir, int maxOpenFiles) {
		File dictionaryDir = new File(storageDir);
		if (!dictionaryDir.exists()) {
			dictionaryDir.mkdirs();
		}
		Options options = getOptions(maxOpenFiles);
		try {
			counter = RocksDB.open(options, storageDir + File.separator + "counter");
		} catch (RocksDBException e) {
			close();
			throw new RuntimeException(e);
		}
	}

	private Options getOptions(int maxOpenFiles) {
		Options options = new Options();
		options.setCreateIfMissing(true);
		options.setMaxOpenFiles(maxOpenFiles);
		options.setWriteBufferSize(64 * 1024 * 1024);
		return options;
	}

	@Override
	public byte[] get(byte[] key) {

		try {
			byte[] count = counter.get(key);
			return count;
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void put(byte[] key, byte[] value) {
		try {
			counter.put(key, value);
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Iterator<byte[]> iterator() {
		return new RocksDBByteIterator(counter.newIterator());
	}

	@Override
	public void flush() {
		try (FlushOptions flushOptions = new FlushOptions()) {
			flushOptions.setWaitForFlush(true);
			counter.flush(flushOptions);
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() {
		if (counter != null) {
			counter.close();
		}
	}

}
