package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.counter.impl;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.rocksdb.RocksIterator;

/**
 * Iterator for the elements of an {@link RocksDBStore}.
 * 
 * @author Philipp Töws
 *
 */
public class RocksDBByteIterator implements Iterator<byte[]>, Closeable {

	private final RocksIterator iterator;

	private byte[] next;

	public RocksDBByteIterator(RocksIterator iterator) {
		this.iterator = iterator;
		iterator.seekToFirst();
		getNext();
	}

	@Override
	public boolean hasNext() {
		return next != null;
	}

	@Override
	public byte[] next() {
		if (!hasNext()) {
			throw new NoSuchElementException();
		}
		byte[] result = next;
		getNext();
		return result;
	}

	private void getNext() {
		if (iterator.isValid()) {
			next = iterator.key();
			iterator.next();
		} else {
			next = null;
			try {
				close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public void close() throws IOException {
		if (iterator != null) {
			iterator.close();
		}

	}

}
