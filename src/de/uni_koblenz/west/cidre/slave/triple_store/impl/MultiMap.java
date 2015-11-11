package de.uni_koblenz.west.cidre.slave.triple_store.impl;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.NavigableSet;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Fun;

import de.uni_koblenz.west.cidre.common.mapDB.MapDBCacheOptions;
import de.uni_koblenz.west.cidre.common.mapDB.MapDBStorageOptions;

/**
 * A MapDB implementation of a multi map.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class MultiMap implements Closeable, AutoCloseable, Iterable<byte[]> {

	private final DB database;

	private final NavigableSet<byte[]> multiMap;

	private final File maxLengthFile;

	private int maxElementLength;

	public MultiMap(MapDBStorageOptions storageType, String databaseFile,
			boolean useTransactions, boolean writeAsynchronously,
			MapDBCacheOptions cacheType, String mapName) {
		assert storageType != MapDBStorageOptions.MEMORY
				|| databaseFile != null;
		DBMaker<?> dbmaker = storageType.getDBMaker(databaseFile);
		if (!useTransactions) {
			dbmaker = dbmaker.transactionDisable().closeOnJvmShutdown();
		}
		if (writeAsynchronously) {
			dbmaker = dbmaker.asyncWriteEnable();
		}
		dbmaker = cacheType.setCaching(dbmaker);
		database = dbmaker.make();

		multiMap = database.createTreeSet(mapName)
				.comparator(Fun.BYTE_ARRAY_COMPARATOR).makeOrGet();

		maxLengthFile = new File(databaseFile + ".maxLength");
		if (maxLengthFile.exists()) {
			loadMaxLength();
		}
	}

	private void loadMaxLength() {
		try (DataInputStream in = new DataInputStream(
				new FileInputStream(maxLengthFile))) {
			maxElementLength = in.readInt();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void saveMaxLength() {
		try (DataOutputStream out = new DataOutputStream(
				new FileOutputStream(maxLengthFile))) {
			out.writeInt(maxElementLength);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public int size() {
		return multiMap.size();
	}

	public boolean isEmpty() {
		return multiMap.isEmpty();
	}

	public boolean containsKey(byte[] prefix) {
		byte[] floor = multiMap.floor(prefix);
		return floor != null && isPrefix(prefix, floor);
	}

	private boolean isPrefix(byte[] prefix, byte[] element) {
		if (prefix.length > element.length) {
			return false;
		}
		for (int i = 0; i < prefix.length; i++) {
			if (prefix[i] != element[i]) {
				return false;
			}
		}
		return true;
	}

	public NavigableSet<byte[]> get(byte[] prefix) {
		return multiMap.subSet(prefix, true, getMaxValue(prefix), true);
	}

	private byte[] getMaxValue(byte[] prefix) {
		byte[] max = new byte[maxElementLength];
		for (int i = 0; i < max.length; i++) {
			max[i] = i < prefix.length ? prefix[i] : Byte.MAX_VALUE;
		}
		return max;
	}

	public void put(byte[] content) {
		if (content.length > maxElementLength) {
			maxElementLength = content.length;
		}
		multiMap.add(content);
	}

	public void removeAll(byte[] prefix) {
		NavigableSet<byte[]> subSet = multiMap.subSet(prefix, true,
				getMaxValue(prefix), true);
		subSet.clear();
	}

	public void remove(byte[] content) {
		multiMap.remove(content);
	}

	@Override
	public Iterator<byte[]> iterator() {
		return multiMap.iterator();
	}

	public void clear() {
		multiMap.clear();
		maxElementLength = 0;
	}

	@Override
	public void close() {
		if (!database.isClosed()) {
			database.close();
		}
		saveMaxLength();
	}
}
