/*
 * This file is part of Koral.
 *
 * Koral is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Koral is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Leser General Public License
 * along with Koral.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright 2016 Daniel Janke
 */
package de.uni_koblenz.west.koral.master.statisticsDB.impl;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import de.uni_koblenz.west.koral.common.utils.NumberConversion;
import de.uni_koblenz.west.koral.master.statisticsDB.GraphStatisticsDatabase;

/**
 * {@link GraphStatisticsDatabase} realized via a random access file.
 *
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class SingleFileGraphStatisticsDatabase implements GraphStatisticsDatabase {

	private RandomAccessFile statistics;

	private final File statisticsFile;

	private final short numberOfChunks;

	public SingleFileGraphStatisticsDatabase(String statisticsDir, short numberOfChunks) {
		File statisticsDirFile = new File(statisticsDir);
		if (!statisticsDirFile.exists()) {
			statisticsDirFile.mkdirs();
		}
		this.numberOfChunks = numberOfChunks;

		statisticsFile = new File(statisticsDirFile.getAbsolutePath() + File.separator + "statistics");
		createStatistics();
	}

	private void createStatistics() {
		try {
			statistics = new RandomAccessFile(statisticsFile, "rw");
		} catch (IOException e) {
			close();
			throw new RuntimeException(e);
		}
	}

	@Override
	public void incrementNumberOfTriplesPerChunk(int chunk) {
		try {
			// number of triples per chunk are stored at the beginning of the file
			long offset = chunk * Long.BYTES;
			statistics.seek(offset);
			try {
				long numberOfTriplesInChunk = statistics.readLong();
				numberOfTriplesInChunk++;
				statistics.seek(offset);
				statistics.writeLong(numberOfTriplesInChunk);
			} catch (EOFException e) {
				if (statistics.getFilePointer() != offset) {
					statistics.seek(offset);
				}
				statistics.writeLong(1);
			}
		} catch (IOException e) {
			close();
			throw new RuntimeException(e);
		}
	}

	@Override
	public void incrementSubjectCount(long subject, int chunk) {
		incrementValue(subject, chunk);
	}

	@Override
	public void incrementPropertyCount(long property, int chunk) {
		incrementValue(property, numberOfChunks + chunk);
	}

	@Override
	public void incrementObjectCount(long object, int chunk) {
		incrementValue(object, (2 * numberOfChunks) + chunk);
	}

	public void incrementRessourceOccurrences(long resource, int chunk) {
		incrementValue(resource, 3 * numberOfChunks);
	}

	private void incrementValue(long resourceID, int column) {
		try {
			long sizeOfRow = (Long.BYTES * numberOfChunks * 3) + Long.BYTES;
			long sizeOfTriplesPerChunk = Long.BYTES * numberOfChunks;
			long offset = sizeOfTriplesPerChunk + ((resourceID - 1) * sizeOfRow) + (column * Long.BYTES);

			statistics.seek(offset);
			try {
				long value = statistics.readLong();
				value++;
				statistics.seek(offset);
				statistics.writeLong(value);
			} catch (EOFException e) {
				if (statistics.getFilePointer() != offset) {
					statistics.seek(offset);
				}
				statistics.writeLong(1);
			}
		} catch (IOException e) {
			close();
			throw new RuntimeException(e);
		}
	}

	@Override
	public long[] getChunkSizes() {
		try {
			statistics.seek(0);
			byte[] row = new byte[Long.BYTES * numberOfChunks];
			try {
				statistics.readFully(row);
			} catch (EOFException e) {
				// the statistics are not completes initialized
			}
			long[] result = new long[numberOfChunks];
			for (int i = 0; i < result.length; i++) {
				result[i] = NumberConversion.bytes2long(row, i * Long.BYTES);
			}
			return result;
		} catch (IOException e) {
			close();
			throw new RuntimeException(e);
		}
	}

	@Override
	public long[] getStatisticsForResource(long id) {
		if (id == 0) {
			return null;
		}
		try {
			int sizeOfRow = (Long.BYTES * numberOfChunks * 3) + Long.BYTES;
			long sizeOfTriplesPerChunk = Long.BYTES * numberOfChunks;
			long offset = sizeOfTriplesPerChunk + ((id - 1) * sizeOfRow);

			byte[] row = new byte[sizeOfRow];
			statistics.seek(offset);
			try {
				statistics.readFully(row);
			} catch (EOFException e) {
				// there exists no values yet
				// Or: The row was not fully written
			}

			long[] result = new long[(numberOfChunks * 3) + 1];
			for (int i = 0; i < result.length; i++) {
				result[i] = NumberConversion.bytes2long(row, i * Long.BYTES);
			}
			return result;
		} catch (IOException e) {
			close();
			throw new RuntimeException(e);
		}
	}

	@Override
	public void clear() {
		close();
		statisticsFile.delete();
		createStatistics();
	}

	public long getMaxId() {
		int sizeOfRow = (Long.BYTES * numberOfChunks * 3) + Long.BYTES;
		long sizeOfTriplesPerChunk = Long.BYTES * numberOfChunks;
		long maxId = 0;
		long fileLength = 0;
		try {
			fileLength = statistics.length();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		maxId = (int) Math.ceil((fileLength - sizeOfTriplesPerChunk) / (double) sizeOfRow);
		return maxId;
	}

	@Override
	public void close() {
		try {
			statistics.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		int sizeOfRow = (Long.BYTES * numberOfChunks * 3) + Long.BYTES;
		long fileLength = 0;
		try {
			fileLength = statistics.length();
			statistics.seek(0);
			sb.append(statistics.readLong() + ",");
			sb.append(statistics.readLong() + ",");
			sb.append(statistics.readLong() + ",");
			sb.append(statistics.readLong());
			sb.append("FP: " + statistics.getFilePointer() + "\n");
			byte[] file = new byte[(int) (fileLength - statistics.getFilePointer())];
			statistics.readFully(file);
			int i = 0;
			int c = 0;
			for (; i < (file.length / sizeOfRow); i++) {
				for (int j = 0; j < ((numberOfChunks * 3) + 1); j++) {
					sb.append(NumberConversion.bytes2long(file, (i * sizeOfRow) + (j * Long.BYTES)) + ", ");
				}
				sb.append("\n");
				c++;
			}
			sb.append("Total Rows: ").append(c).append("\n");
			sb.append("Rest:\n");
			for (int j = i * sizeOfRow; j < file.length; j++) {
				sb.append(file[j] + ", ");
			}
			sb.append("\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return sb.toString();
//		StringBuilder sb = new StringBuilder();
//		sb.append("TriplesPerChunk ");
//		for (long l : getChunkSizes()) {
//			sb.append("\t").append(l);
//		}
//		sb.append("\n");
//		sb.append("ResourceID");
//		for (int i = 0; i < numberOfChunks; i++) {
//			sb.append(";").append("subjectInChunk").append(i);
//		}
//		for (int i = 0; i < numberOfChunks; i++) {
//			sb.append(";").append("propertyInChunk").append(i);
//		}
//		for (int i = 0; i < numberOfChunks; i++) {
//			sb.append(";").append("objectInChunk").append(i);
//		}
//		sb.append(";").append("overallOccurrance");
//		try {
//			int sizeOfRow = (Long.BYTES * numberOfChunks * 3) + Long.BYTES;
//			long sizeOfTriplesPerChunk = Long.BYTES * numberOfChunks;
//			long maxId = (statistics.length() - sizeOfTriplesPerChunk) / sizeOfRow;
//			for (long id = 1; id <= maxId; id++) {
//				sb.append("\n");
//				sb.append(id);
//				for (long value : getStatisticsForResource(id)) {
//					sb.append(";").append(value);
//				}
//			}
//		} catch (IOException e) {
//			throw new RuntimeException(e);
//		}
//		return sb.toString();
	}

}
