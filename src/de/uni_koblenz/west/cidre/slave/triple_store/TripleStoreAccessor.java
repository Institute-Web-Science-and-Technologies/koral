package de.uni_koblenz.west.cidre.slave.triple_store;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.query.Mapping;
import de.uni_koblenz.west.cidre.common.query.MappingRecycleCache;
import de.uni_koblenz.west.cidre.common.query.TriplePattern;
import de.uni_koblenz.west.cidre.slave.triple_store.impl.MapDBTripleStore;

/**
 * Provides access to the local triple store. I.e., methods to store all triples
 * of a graph file and methods to lookup triples again.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class TripleStoreAccessor implements Closeable, AutoCloseable {

	private final Logger logger;

	private final MapDBTripleStore tripleStore;

	public TripleStoreAccessor(Configuration conf, Logger logger) {
		this.logger = logger;
		tripleStore = new MapDBTripleStore(conf.getTripleStoreStorageType(),
				conf.getTripleStoreDir(), conf.useTransactionsForTripleStore(),
				conf.isTripleStoreAsynchronouslyWritten(),
				conf.getTripleStoreCacheType());
		if (logger != null) {
			// TODO remove
			logger.info("\n" + tripleStore.toString());
		}
	}

	public void storeTriples(File file) {
		try (DataInputStream in = new DataInputStream(new BufferedInputStream(
				new GZIPInputStream(new FileInputStream(file))));) {
			long alreadyLoadedTriples = 0;
			while (true) {
				long subject;
				try {
					subject = in.readLong();
				} catch (EOFException e) {
					break;
				}
				long property = in.readLong();
				long object = in.readLong();
				short length = in.readShort();
				byte[] containment = new byte[length];
				in.readFully(containment);
				tripleStore.storeTriple(subject, property, object, containment);
				alreadyLoadedTriples++;
				if (logger != null && alreadyLoadedTriples % 10000 == 0) {
					logger.finer("loaded " + alreadyLoadedTriples + " triples");
				}
			}
			if (logger != null) {
				logger.finer("finished loading of " + alreadyLoadedTriples
						+ " triples from file " + file.getAbsolutePath());
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public Iterable<Mapping> lookup(MappingRecycleCache cache,
			TriplePattern triplePattern) {
		return tripleStore.lookup(cache, triplePattern);
	}

	public void clear() {
		tripleStore.clear();
	}

	@Override
	public void close() {
		tripleStore.close();
	}

}
