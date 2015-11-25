package de.uni_koblenz.west.cidre.slave.triple_store.impl;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.NavigableSet;

import de.uni_koblenz.west.cidre.common.mapDB.MapDBCacheOptions;
import de.uni_koblenz.west.cidre.common.mapDB.MapDBStorageOptions;
import de.uni_koblenz.west.cidre.common.query.Mapping;
import de.uni_koblenz.west.cidre.common.query.MappingRecycleCache;
import de.uni_koblenz.west.cidre.common.query.TriplePattern;
import de.uni_koblenz.west.cidre.common.utils.NumberConversion;
import de.uni_koblenz.west.cidre.slave.triple_store.TripleStore;

/**
 * A MapDB implementation of the local triple store. Each triple is stored in
 * the SPO, OSP, and POS index. Each index is realized by a {@link MultiMap}.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class MapDBTripleStore implements TripleStore {

	private final MultiMap spo;

	private final MultiMap osp;

	private final MultiMap pos;

	public MapDBTripleStore(MapDBStorageOptions storageType,
			String tripleStoreDir, boolean useTransactions,
			boolean writeAsynchronously, MapDBCacheOptions cacheType) {
		File dir = new File(tripleStoreDir);
		if (!dir.exists()) {
			dir.mkdirs();
		}
		spo = new MultiMap(storageType,
				tripleStoreDir + File.separatorChar + "spo", useTransactions,
				writeAsynchronously, cacheType, "spo");
		osp = new MultiMap(storageType,
				tripleStoreDir + File.separatorChar + "osp", useTransactions,
				writeAsynchronously, cacheType, "osp");
		pos = new MultiMap(storageType,
				tripleStoreDir + File.separatorChar + "pos", useTransactions,
				writeAsynchronously, cacheType, "pos");
	}

	@Override
	public void storeTriple(long subject, long property, long object,
			byte[] containment) {
		ByteBuffer buffer = ByteBuffer
				.allocate(3 * Long.BYTES + containment.length);
		spo.put(buffer.putLong(subject).putLong(property).putLong(object)
				.put(containment).array());
		buffer.flip();
		osp.put(buffer.putLong(object).putLong(subject).putLong(property)
				.put(containment).array());
		buffer.flip();
		pos.put(buffer.putLong(property).putLong(object).putLong(subject)
				.put(containment).array());
	}

	@Override
	public Iterable<Mapping> lookup(MappingRecycleCache cache,
			TriplePattern triplePattern) {
		byte[] queryPrefix = null;
		NavigableSet<byte[]> matches = null;
		IndexType indexType = null;
		switch (triplePattern.getType()) {
		case ___:
			queryPrefix = new byte[0];
			matches = spo.get(queryPrefix);
			indexType = IndexType.SPO;
			break;
		case S__:
			queryPrefix = NumberConversion
					.long2bytes(triplePattern.getSubject());
			matches = spo.get(queryPrefix);
			indexType = IndexType.SPO;
			break;
		case _P_:
			queryPrefix = NumberConversion
					.long2bytes(triplePattern.getProperty());
			matches = pos.get(queryPrefix);
			indexType = IndexType.POS;
			break;
		case __O:
			queryPrefix = NumberConversion
					.long2bytes(triplePattern.getObject());
			matches = osp.get(queryPrefix);
			indexType = IndexType.OSP;
			break;
		case SP_:
			queryPrefix = ByteBuffer.allocate(2 * Long.BYTES)
					.putLong(triplePattern.getSubject())
					.putLong(triplePattern.getProperty()).array();
			matches = spo.get(queryPrefix);
			indexType = IndexType.SPO;
			break;
		case S_O:
			queryPrefix = ByteBuffer.allocate(2 * Long.BYTES)
					.putLong(triplePattern.getObject())
					.putLong(triplePattern.getSubject()).array();
			matches = osp.get(queryPrefix);
			indexType = IndexType.OSP;
			break;
		case _PO:
			queryPrefix = ByteBuffer.allocate(2 * Long.BYTES)
					.putLong(triplePattern.getProperty())
					.putLong(triplePattern.getObject()).array();
			matches = pos.get(queryPrefix);
			indexType = IndexType.POS;
			break;
		case SPO:
			queryPrefix = ByteBuffer.allocate(3 * Long.BYTES)
					.putLong(triplePattern.getSubject())
					.putLong(triplePattern.getProperty())
					.putLong(triplePattern.getObject()).array();
			matches = spo.get(queryPrefix);
			indexType = IndexType.SPO;
			break;
		}
		return new MappingIteratorWrapper(cache, triplePattern, indexType,
				matches.iterator());
	}

	@Override
	public void clear() {
		spo.clear();
		osp.clear();
		pos.clear();
	}

	@Override
	public void close() {
		spo.close();
		osp.close();
		pos.close();
	}

}
