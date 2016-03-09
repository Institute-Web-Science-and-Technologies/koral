package de.uni_koblenz.west.cidre.common.utils;

import java.io.Closeable;
import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.logging.Logger;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import de.uni_koblenz.west.cidre.common.mapDB.MapDBCacheOptions;
import de.uni_koblenz.west.cidre.common.mapDB.MapDBStorageOptions;
import de.uni_koblenz.west.cidre.common.query.Mapping;
import de.uni_koblenz.west.cidre.common.query.MappingRecycleCache;

/**
 * Instances are used for joins. It caches the mappings received from one
 * specific child of the join operation.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class JoinMappingCache implements Closeable, Iterable<Mapping> {

	// TODO remove
	public static Logger logger;

	private final MappingRecycleCache recycleCache;

	private final File mapFolder;

	private final DB database;

	private final NavigableSet<byte[]> multiMap;

	private final long[] variables;

	private final int[] joinVarIndices;

	private int size;

	/**
	 * @param storageType
	 * @param useTransactions
	 * @param writeAsynchronously
	 * @param cacheType
	 * @param cacheDirectory
	 * @param recycleCache
	 * @param uniqueFileNameSuffix
	 * @param mappingVariables
	 * @param variableComparisonOrder
	 *            must contain all variables of the mapping. First variable has
	 *            index 0. The join variables must occur first!
	 * @param numberOfJoinVars
	 */
	public JoinMappingCache(MapDBStorageOptions storageType,
			boolean useTransactions, boolean writeAsynchronously,
			MapDBCacheOptions cacheType, File cacheDirectory,
			MappingRecycleCache recycleCache, String uniqueFileNameSuffix,
			long[] mappingVariables, int[] variableComparisonOrder,
			int numberOfJoinVars) {
		assert storageType != MapDBStorageOptions.MEMORY
				|| cacheDirectory != null;
		this.recycleCache = recycleCache;
		variables = mappingVariables;
		joinVarIndices = new int[numberOfJoinVars];
		for (int i = 0; i < numberOfJoinVars; i++) {
			joinVarIndices[i] = variableComparisonOrder[i];
		}
		mapFolder = new File(cacheDirectory.getAbsolutePath() + File.separator
				+ uniqueFileNameSuffix);
		if (!mapFolder.exists()) {
			mapFolder.mkdirs();
		}
		DBMaker<?> dbmaker = storageType.getDBMaker(mapFolder.getAbsolutePath()
				+ File.separator + uniqueFileNameSuffix);
		if (!useTransactions) {
			dbmaker = dbmaker.transactionDisable().closeOnJvmShutdown();
		}
		if (writeAsynchronously) {
			dbmaker = dbmaker.asyncWriteEnable();
		}
		dbmaker = cacheType.setCaching(dbmaker);
		database = dbmaker.make();

		if (logger != null) {
			// TODO remove
			JoinComparator.logger = logger;
		}

		multiMap = database.createTreeSet(uniqueFileNameSuffix)
				.comparator(new JoinComparator(variableComparisonOrder))
				.makeOrGet();

		size = 0;
	}

	public boolean isEmpty() {
		return size() == 0;
	}

	public long size() {
		return size;
	}

	public void add(Mapping mapping) {
		size++;
		byte[] newMapping = new byte[mapping.getLengthOfMappingInByteArray()];
		System.arraycopy(mapping.getByteArray(),
				mapping.getFirstIndexOfMappingInByteArray(), newMapping, 0,
				mapping.getLengthOfMappingInByteArray());
		if (logger != null) {
			// TODO remove
			logger.info("\nadding=" + mapping.toString() + " from="
					+ NumberConversion.id2description(
							NumberConversion.bytes2long(mapping.getByteArray(),
									mapping.getFirstIndexOfMappingInByteArray()
											+ Byte.BYTES + Long.BYTES))
					+ " isClosed=" + database.isClosed());
		}
		multiMap.add(newMapping);
		if (logger != null) {
			// TODO remove
			logger.info("\nadded=" + mapping.toString());
		}
	}

	public Iterator<Mapping> getMatchCandidates(Mapping mapping,
			long[] mappingVars) {
		int headerSize = Mapping.getHeaderSize();
		byte[] min = new byte[headerSize + variables.length * Long.BYTES
				+ mapping.getNumberOfContainmentBytes()];
		byte[] max = new byte[headerSize + variables.length * Long.BYTES
				+ mapping.getNumberOfContainmentBytes()];
		// set join vars
		for (int varIndex : joinVarIndices) {
			NumberConversion.long2bytes(
					mapping.getValue(variables[varIndex], mappingVars), min,
					headerSize + varIndex * Long.BYTES);
			NumberConversion.long2bytes(
					mapping.getValue(variables[varIndex], mappingVars), max,
					headerSize + varIndex * Long.BYTES);
		}
		// set non join vars
		for (int i = 0; i < min.length; i++) {
			if (isFirstIndexOfAJoinVar(i)) {
				i += Long.BYTES - 1;
			} else {
				min[i] = Byte.MIN_VALUE;
				max[i] = Byte.MAX_VALUE;
			}
		}
		if (logger != null) {
			// TODO remove
			logger.info("\nfind match candidates for="
					+ mapping.toString(mappingVars));
		}
		NavigableSet<byte[]> subset = multiMap.subSet(min, true, max, true);
		if (logger != null) {
			// TODO remove
			logger.info("\nsubset created=" + subset.toString());
		}
		return new MappingIteratorWrapper(subset.iterator(), recycleCache);
	}

	private boolean isFirstIndexOfAJoinVar(int index) {
		int headerSize = Mapping.getHeaderSize();
		for (int varIndex : joinVarIndices) {
			if (index == headerSize + varIndex * Long.BYTES) {
				return true;
			}
		}
		return false;
	}

	@Override
	public Iterator<Mapping> iterator() {
		return new MappingIteratorWrapper(multiMap.iterator(), recycleCache);
	}

	@Override
	public void close() {
		if (!database.isClosed()) {
			database.close();
		}
		if (mapFolder.exists()) {
			for (File file : mapFolder.listFiles()) {
				file.delete();
			}
			mapFolder.delete();
		}
	}

	public static class JoinComparator
			implements Comparator<byte[]>, Serializable {

		// TODO remove
		public static Logger logger;

		private static final long serialVersionUID = -7360345226100972052L;

		private final int offset = Mapping.getHeaderSize();

		private final int[] comparisonOrder;

		/**
		 * ComparisonOrder must contain all variables of the mapping. First
		 * variable has index 0.
		 * 
		 * @param comparisonOrder
		 */
		public JoinComparator(int[] comparisonOrder) {
			this.comparisonOrder = comparisonOrder;
		}

		@Override
		public int compare(byte[] thisMapping, byte[] otherMapping) {
			if (thisMapping == otherMapping) {
				if (logger != null) {
					// TODO remove
					logger.info("\n" + Arrays.toString(thisMapping) + "\n==\n"
							+ Arrays.toString(otherMapping));
				}
				return 0;
			}
			for (int var : comparisonOrder) {
				int comparison = longCompare(getVar(var, thisMapping),
						getVar(var, otherMapping));
				if (comparison != 0) {
					if (logger != null) {
						// TODO remove
						logger.info("\n" + Arrays.toString(thisMapping) + "\n"
								+ (comparison < 0 ? "<" : ">") + "\n"
								+ Arrays.toString(otherMapping));
					}
					return comparison;
				}
			}
			// compare containment information
			final int len = Math.min(thisMapping.length, otherMapping.length);
			for (int i = offset
					+ comparisonOrder.length * Long.BYTES; i < len; i++) {
				if (thisMapping[i] < otherMapping[i]) {
					if (logger != null) {
						// TODO remove
						logger.info("\n" + Arrays.toString(thisMapping)
								+ "\n<\n" + Arrays.toString(otherMapping));
					}
					return -1;
				}
				if (thisMapping[i] > otherMapping[i]) {
					if (logger != null) {
						// TODO remove
						logger.info("\n" + Arrays.toString(thisMapping)
								+ "\n>\n" + Arrays.toString(otherMapping));
					}
					return 1;
				}
			}
			int comparison = intCompare(thisMapping.length,
					otherMapping.length);
			if (logger != null) {
				// TODO remove
				logger.info("\n" + Arrays.toString(thisMapping) + "\n"
						+ (comparison < 0 ? "<"
								: (comparison == 0 ? "==" : ">"))
						+ "\n" + Arrays.toString(otherMapping));
			}
			return comparison;
		}

		private long getVar(int varIndex, byte[] mapping) {
			return NumberConversion.bytes2long(mapping,
					offset + varIndex * Long.BYTES);
		}

		private int intCompare(int x, int y) {
			return (x < y) ? -1 : ((x == y) ? 0 : 1);
		}

		private int longCompare(long x, long y) {
			return (x < y) ? -1 : ((x == y) ? 0 : 1);
		}

	}

}
