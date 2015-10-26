package de.uni_koblenz.west.cidre.master.statisticsDB.impl;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import de.uni_koblenz.west.cidre.common.mapDB.MapDBCacheOptions;
import de.uni_koblenz.west.cidre.common.mapDB.MapDBDataStructureOptions;
import de.uni_koblenz.west.cidre.common.mapDB.MapDBStorageOptions;
import de.uni_koblenz.west.cidre.master.statisticsDB.GraphStatisticsDatabase;

public class MapDBGraphStatisticsDatabase implements GraphStatisticsDatabase {

	private final File persistenceFile;

	private long[] numberOfTriplesPerChunk;

	private long[] ownerLoad;

	private final short numberOfChunks;

	public MapDBGraphStatisticsDatabase(MapDBStorageOptions storageType,
			MapDBDataStructureOptions dataStructure, String dir,
			boolean useTransactions, boolean writeAsynchronously,
			MapDBCacheOptions cacheType, short numberOfChunks) {
		File statisticsDir = new File(dir);
		if (!statisticsDir.exists()) {
			statisticsDir.mkdirs();
		}

		this.numberOfChunks = numberOfChunks;
		persistenceFile = new File(
				dir + File.separatorChar + "persistence.bin");
		if (persistenceFile.exists()) {
			loadPersistenceFile();
		} else {
			numberOfTriplesPerChunk = new long[numberOfChunks];
			ownerLoad = new long[numberOfChunks];
		}
		// TODO Auto-generated constructor stub
	}

	private void loadPersistenceFile() {
		try (DataInputStream in = new DataInputStream(new BufferedInputStream(
				new FileInputStream(persistenceFile)));) {
			numberOfTriplesPerChunk = readLongArray(in);
			ownerLoad = readLongArray(in);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}

	private long[] readLongArray(DataInputStream in) throws IOException {
		long[] result;
		short length = in.readShort();
		result = new long[length];
		for (int i = 0; i < result.length; i++) {
			result[i] = in.readLong();
		}
		return result;
	}

	@Override
	public void incrementSubjectCount(long subject, int chunk) {
		// TODO Auto-generated method stub

	}

	@Override
	public void incrementPropertyCount(long property, int chunk) {
		// TODO Auto-generated method stub

	}

	@Override
	public void incrementObjectCount(long object, int chunk) {
		// TODO Auto-generated method stub

	}

	@Override
	public void incrementRessourceOccurrences(long ressource, int chunk) {
		// TODO Auto-generated method stub

	}

	@Override
	public void inrementNumberOfTriplesPerChunk(int chunk) {
		// TODO Auto-generated method stub

	}

	@Override
	public long[] getStatisticsForResource(long id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long[] getChunkSizes() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long[] getOwnerLoad() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long setOwner(long id, short owner) {
		// TODO if already set, do not look into tables
		ownerLoad[owner]++;
		return 0;
	}

	@Override
	public void clear() {
		if (persistenceFile.exists()) {
			persistenceFile.delete();
		}
		for (int i = 0; i < numberOfTriplesPerChunk.length; i++) {
			numberOfTriplesPerChunk[i] = 0;
			ownerLoad[i] = 0;
		}
		// TODO Auto-generated method stub

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		persistNumberOfTriplesPerChunk();
	}

	private void persistNumberOfTriplesPerChunk() {
		try (DataOutputStream out = new DataOutputStream(
				new BufferedOutputStream(
						new FileOutputStream(persistenceFile)));) {
			// persist number of triples per chunk
			out.writeShort((short) numberOfTriplesPerChunk.length);
			for (long l : numberOfTriplesPerChunk) {
				out.writeLong(l);
			}
			// persist ownerLoad
			out.writeShort((short) ownerLoad.length);
			for (long l : ownerLoad) {
				out.writeLong(l);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
