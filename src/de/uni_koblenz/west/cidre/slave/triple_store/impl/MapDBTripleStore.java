package de.uni_koblenz.west.cidre.slave.triple_store.impl;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.NavigableSet;

import de.uni_koblenz.west.cidre.common.mapDB.MapDBCacheOptions;
import de.uni_koblenz.west.cidre.common.mapDB.MapDBStorageOptions;
import de.uni_koblenz.west.cidre.common.query.Mapping;
import de.uni_koblenz.west.cidre.common.query.MappingRecycleCache;
import de.uni_koblenz.west.cidre.common.query.TriplePattern;
import de.uni_koblenz.west.cidre.slave.triple_store.TripleStore;

public class MapDBTripleStore implements TripleStore {

	private final MultiMap spo;

	private final MultiMap osp;

	private final MultiMap pos;

	public MapDBTripleStore(MapDBStorageOptions storageType,
			File tripleStoreDir, boolean useTransactions,
			boolean writeAsynchronously, MapDBCacheOptions cacheType) {
		if (!tripleStoreDir.exists()) {
			tripleStoreDir.mkdirs();
		}
		spo = new MultiMap(storageType,
				tripleStoreDir.getAbsolutePath() + File.separatorChar + "spo",
				useTransactions, writeAsynchronously, cacheType, "spo");
		osp = new MultiMap(storageType,
				tripleStoreDir.getAbsolutePath() + File.separatorChar + "osp",
				useTransactions, writeAsynchronously, cacheType, "osp");
		pos = new MultiMap(storageType,
				tripleStoreDir.getAbsolutePath() + File.separatorChar + "pos",
				useTransactions, writeAsynchronously, cacheType, "pos");
	}

	@Override
	public void storeTriple(long subject, long property, long object,
			byte[] containment) {
		spo.put(ByteBuffer.allocate(3 * 8 + containment.length).putLong(subject)
				.putLong(property).putLong(object).put(containment).array());
		osp.put(ByteBuffer.allocate(3 * 8 + containment.length).putLong(object)
				.putLong(subject).putLong(property).put(containment).array());
		pos.put(ByteBuffer.allocate(3 * 8 + containment.length)
				.putLong(property).putLong(object).putLong(subject)
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
			queryPrefix = ByteBuffer.allocate(8)
					.putLong(triplePattern.getSubject()).array();
			matches = spo.get(queryPrefix);
			indexType = IndexType.SPO;
			break;
		case _P_:
			queryPrefix = ByteBuffer.allocate(8)
					.putLong(triplePattern.getProperty()).array();
			matches = pos.get(queryPrefix);
			indexType = IndexType.POS;
			break;
		case __O:
			queryPrefix = ByteBuffer.allocate(8)
					.putLong(triplePattern.getObject()).array();
			matches = osp.get(queryPrefix);
			indexType = IndexType.OSP;
			break;
		case SP_:
			queryPrefix = ByteBuffer.allocate(16)
					.putLong(triplePattern.getSubject())
					.putLong(triplePattern.getProperty()).array();
			matches = spo.get(queryPrefix);
			indexType = IndexType.SPO;
			break;
		case S_O:
			queryPrefix = ByteBuffer.allocate(16)
					.putLong(triplePattern.getObject())
					.putLong(triplePattern.getSubject()).array();
			matches = osp.get(queryPrefix);
			indexType = IndexType.OSP;
			break;
		case _PO:
			queryPrefix = ByteBuffer.allocate(16)
					.putLong(triplePattern.getProperty())
					.putLong(triplePattern.getObject()).array();
			matches = pos.get(queryPrefix);
			indexType = IndexType.POS;
			break;
		case SPO:
			queryPrefix = ByteBuffer.allocate(24)
					.putLong(triplePattern.getSubject())
					.putLong(triplePattern.getProperty())
					.putLong(triplePattern.getObject()).array();
			matches = spo.get(queryPrefix);
			indexType = IndexType.SPO;
			break;
		}
		return new MappingIteratorWrapper(cache, indexType, triplePattern,
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
