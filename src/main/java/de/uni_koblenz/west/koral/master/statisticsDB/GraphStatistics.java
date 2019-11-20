/*
 * This file is part of Koral.
 *
 * Koral is free software: you can redistribute it and/or modify it under the terms of the GNU
 * Lesser General Public License as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Koral is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
 * General Public License for more details.
 *
 * You should have received a copy of the GNU Leser General Public License along with Koral. If not,
 * see <http://www.gnu.org/licenses/>.
 *
 * Copyright 2016 Daniel Janke
 */
package de.uni_koblenz.west.koral.master.statisticsDB;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.BitSet;
import java.util.logging.Logger;

import de.uni_koblenz.west.koral.common.config.impl.Configuration;
import de.uni_koblenz.west.koral.common.io.EncodedFileInputStream;
import de.uni_koblenz.west.koral.common.io.EncodedFileOutputStream;
import de.uni_koblenz.west.koral.common.io.EncodingFileFormat;
import de.uni_koblenz.west.koral.common.io.Statement;
import de.uni_koblenz.west.koral.common.utils.NumberConversion;
import de.uni_koblenz.west.koral.master.dictionary.DictionaryEncoder;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.MultiFileGraphStatisticsDatabase;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.SubbenchmarkManager;
import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log.StorageLogWriter;
import playground.StatisticsDBTest;

/**
 * Stores statistical information about the occurrence of resources in the different graph chunks. It receives its data
 * from {@link DictionaryEncoder}.
 *
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class GraphStatistics implements Closeable {

	@SuppressWarnings("unused")
	private final Logger logger;

	private final GraphStatisticsDatabase database;

	private final int numberOfChunks;

	public GraphStatistics(Configuration conf, short numberOfChunks, Logger logger) {
		this.logger = logger;
		this.numberOfChunks = numberOfChunks;
		// TODO enable
		// database = new SQLiteGraphStatisticsDatabase(conf.getStatisticsDir(),
		// numberOfChunks);
		database = new MultiFileGraphStatisticsDatabase(conf.getStatisticsDir(true), numberOfChunks, logger);
	}

	public GraphStatistics(GraphStatisticsDatabase database, short numberOfChunks, Logger logger) {
		this.logger = logger;
		this.numberOfChunks = numberOfChunks;
		this.database = database;
	}

	public void collectStatistics(File[] encodedChunks) {
		clear();
		for (int i = 0; i < encodedChunks.length; i++) {
			collectStatistics(i, encodedChunks[i]);
		}
	}

	private void collectStatistics(int chunkIndex, File chunk) {
		if (chunk == null) {
			return;
		}
		try (EncodedFileInputStream in = new EncodedFileInputStream(EncodingFileFormat.EEE, chunk);) {
			long start = System.nanoTime();
			for (Statement statement : in) {
				SubbenchmarkManager.getInstance().addInputReadTime(System.nanoTime() - start);
				count(statement.getSubjectAsLong(), statement.getPropertyAsLong(), statement.getObjectAsLong(),
						chunkIndex);
				start = System.nanoTime();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		System.out.println("Chunk " + chunkIndex + " done.");
		if (StatisticsDBTest.ENABLE_STORAGE_LOGGING) {
			StorageLogWriter.getInstance().logChunkSwitchEvent();
		}
	}

	public void count(long subject, long property, long object, int chunk) {
		// Remove ownership bits
		database.incrementSubjectCount(subject & 0x00_00_FF_FF_FF_FF_FF_FFL, chunk);
		database.incrementPropertyCount(property & 0x00_00_FF_FF_FF_FF_FF_FFL, chunk);
		database.incrementObjectCount(object & 0x00_00_FF_FF_FF_FF_FF_FFL, chunk);
		database.incrementNumberOfTriplesPerChunk(chunk);
	}

	public long[] getChunkSizes() {
		return database.getChunkSizes();
	}

	public File[] adjustOwnership(File[] encodedChunks, File workingDir) {
		File[] result = getAdjustedFiles(workingDir);
		for (int i = 0; i < encodedChunks.length; i++) {
			if (encodedChunks[i] == null) {
				result[i] = null;
				continue;
			}
			try (EncodedFileInputStream in = new EncodedFileInputStream(EncodingFileFormat.EEE, encodedChunks[i]);
					EncodedFileOutputStream out = new EncodedFileOutputStream(result[i]);) {
				for (Statement statement : in) {
					Statement newStatement = Statement.getStatement(EncodingFileFormat.EEE,
							NumberConversion.long2bytes(getIDWithOwner(statement.getSubjectAsLong())),
							NumberConversion.long2bytes(getIDWithOwner(statement.getPropertyAsLong())),
							NumberConversion.long2bytes(getIDWithOwner(statement.getObjectAsLong())),
							statement.getContainment());
					out.writeStatement(newStatement);
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		for (File file : encodedChunks) {
			if (file != null) {
				file.delete();
			}
		}
		return result;
	}

	public File[] getAdjustedFiles(File workingDir) {
		File[] chunkFiles = new File[numberOfChunks];
		for (int i = 0; i < chunkFiles.length; i++) {
			chunkFiles[i] = new File(workingDir.getAbsolutePath() + File.separatorChar + "chunk" + i + ".adj.gz");
		}
		return chunkFiles;
	}

	public long getIDWithOwner(long id) {
		long newID = getOwner(id);
		newID = newID << 48;
		newID |= id;
		return newID;
	}

	private short getOwner(long id) {
		short owner = (short) (id >>> 48);
		if (owner != 0) {
			return owner;
		}
		long[] statistics = database.getStatisticsForResource(id);
		if (statistics == null) {
			return owner;
		}

		BitSet ownerCandidates = new BitSet(numberOfChunks);
		ownerCandidates.set(0, numberOfChunks, true);

		// check occurrence as subject
		argMax(statistics, 0 * numberOfChunks, ownerCandidates);
		assert ownerCandidates.size() > 0;
		if (ownerCandidates.size() == 1) {
			return (short) ownerCandidates.nextSetBit(0);
		}

		// check occurrence as object
		argMax(statistics, 2 * numberOfChunks, ownerCandidates);
		assert ownerCandidates.size() > 0;
		if (ownerCandidates.size() == 1) {
			return (short) ownerCandidates.nextSetBit(0);
		}

		// check occurrence as property
		argMax(statistics, 1 * numberOfChunks, ownerCandidates);
		assert ownerCandidates.size() > 0;
		if (ownerCandidates.size() == 1) {
			return (short) ownerCandidates.nextSetBit(0);
		}

		// select first compute node
		return (short) ownerCandidates.nextSetBit(0);
	}

	private void argMax(long[] statistics, int offset, BitSet ownerCandidates) {
		long max = Long.MIN_VALUE;
		int currentPos = ownerCandidates.nextSetBit(0);
		while (currentPos > -1) {
			if (statistics[offset + currentPos] < max) {
				ownerCandidates.clear(currentPos);
			} else if (statistics[offset + currentPos] > max) {
				max = statistics[offset + currentPos];
				ownerCandidates.set(0, currentPos, false);
			}
			currentPos = ownerCandidates.nextSetBit(currentPos + 1);
		}
	}

	public long getSubjectFrequency(long subject, int slave) {
		subject = subject & 0x00_00_ff_ff_ff_ff_ff_ffL;
		long[] statisticsForResource = database.getStatisticsForResource(subject);
		if (statisticsForResource == null) {
			// this resource does not occur
			return 0;
		}
		return statisticsForResource[(0 * numberOfChunks) + slave];
	}

	public long getPropertyFrequency(long property, int slave) {
		property = property & 0x00_00_ff_ff_ff_ff_ff_ffL;
		long[] statisticsForResource = database.getStatisticsForResource(property);
		if (statisticsForResource == null) {
			// this resource does not occur
			return 0;
		}
		return statisticsForResource[(1 * numberOfChunks) + slave];
	}

	public long getObjectFrequency(long object, int slave) {
		object = object & 0x00_00_ff_ff_ff_ff_ff_ffL;
		long[] statisticsForResource = database.getStatisticsForResource(object);
		if (statisticsForResource == null) {
			// this resource does not occur
			return 0;
		}
		return statisticsForResource[(2 * numberOfChunks) + slave];
	}

	public long getTotalSubjectFrequency(long subject) {
		subject = subject & 0x00_00_ff_ff_ff_ff_ff_ffL;
		long totalFrequency = 0;
		long[] statisticsForResource = database.getStatisticsForResource(subject);
		if (statisticsForResource == null) {
			// this resource does not occur
			return 0;
		}
		for (int slave = 0; slave < numberOfChunks; slave++) {
			totalFrequency += statisticsForResource[(0 * numberOfChunks) + slave];
		}
		return totalFrequency;
	}

	public long getTotalPropertyFrequency(long property) {
		property = property & 0x00_00_ff_ff_ff_ff_ff_ffL;
		long totalFrequency = 0;
		long[] statisticsForResource = database.getStatisticsForResource(property);
		if (statisticsForResource == null) {
			// this resource does not occur
			return 0;
		}
		for (int slave = 0; slave < numberOfChunks; slave++) {
			totalFrequency += statisticsForResource[(1 * numberOfChunks) + slave];
		}
		return totalFrequency;
	}

	public long getTotalObjectFrequency(long object) {
		object = object & 0x00_00_ff_ff_ff_ff_ff_ffL;
		long totalFrequency = 0;
		long[] statisticsForResource = database.getStatisticsForResource(object);
		if (statisticsForResource == null) {
			// this resource does not occur
			return 0;
		}
		for (int slave = 0; slave < numberOfChunks; slave++) {
			totalFrequency += statisticsForResource[(2 * numberOfChunks) + slave];
		}
		return totalFrequency;
	}

	public int getNumberOfChunks() {
		return numberOfChunks;
	}

	public void clear() {
		database.clear();
	}

	@Override
	public void close() {
		database.close();
	}

	@Override
	public String toString() {
		return database.toString();
	}

}
