package de.uni_koblenz.west.cidre.slave.triple_store.impl;

import java.io.File;

import de.uni_koblenz.west.cidre.common.mapDB.MapDBCacheOptions;
import de.uni_koblenz.west.cidre.common.mapDB.MapDBStorageOptions;
import de.uni_koblenz.west.cidre.common.query.Mapping;
import de.uni_koblenz.west.cidre.common.query.TriplePattern;
import de.uni_koblenz.west.cidre.slave.triple_store.TripleStore;

public class MapDBTripleStore implements TripleStore {

	private final MultiMap1x3 s_po;

	private final MultiMap1x3 o_sp;

	private final MultiMap1x3 p_os;

	private final MultiMap2x2 sp_o;

	private final MultiMap2x2 os_p;

	private final MultiMap2x2 po_s;

	public MapDBTripleStore(MapDBStorageOptions storageType,
			File tripleStoreDir, boolean useTransactions,
			boolean writeAsynchronously, MapDBCacheOptions cacheType) {
		if (!tripleStoreDir.exists()) {
			tripleStoreDir.mkdirs();
		}
		s_po = new MultiMap1x3(storageType,
				tripleStoreDir.getAbsolutePath() + File.separatorChar + "s_po",
				useTransactions, writeAsynchronously, cacheType, "s_po");
		o_sp = new MultiMap1x3(storageType,
				tripleStoreDir.getAbsolutePath() + File.separatorChar + "o_sp",
				useTransactions, writeAsynchronously, cacheType, "o_sp");
		p_os = new MultiMap1x3(storageType,
				tripleStoreDir.getAbsolutePath() + File.separatorChar + "p_os",
				useTransactions, writeAsynchronously, cacheType, "p_os");
		sp_o = new MultiMap2x2(storageType,
				tripleStoreDir.getAbsolutePath() + File.separatorChar + "sp_o",
				useTransactions, writeAsynchronously, cacheType, "sp_o");
		os_p = new MultiMap2x2(storageType,
				tripleStoreDir.getAbsolutePath() + File.separatorChar + "os_p",
				useTransactions, writeAsynchronously, cacheType, "os_p");
		po_s = new MultiMap2x2(storageType,
				tripleStoreDir.getAbsolutePath() + File.separatorChar + "po_s",
				useTransactions, writeAsynchronously, cacheType, "po_s");
	}

	@Override
	public void storeTriple(long subject, long property, long object,
			byte[] containment) {
		s_po.put(subject, property, object, containment);
		sp_o.put(subject, property, object, containment);
		o_sp.put(object, subject, property, containment);
		os_p.put(object, subject, property, containment);
		p_os.put(property, object, subject, containment);
		po_s.put(property, object, subject, containment);
	}

	@Override
	public Iterable<Mapping> lookup(TriplePattern triplePattern) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void clear() {
		s_po.clear();
		o_sp.clear();
		p_os.clear();
		sp_o.clear();
		os_p.clear();
		po_s.clear();
	}

	@Override
	public void close() {
		s_po.close();
		o_sp.close();
		p_os.close();
		sp_o.close();
		os_p.close();
		po_s.close();
	}

}
