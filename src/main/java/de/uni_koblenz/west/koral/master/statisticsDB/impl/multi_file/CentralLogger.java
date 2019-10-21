package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import playground.StatisticsDBTest;

/**
 * General-purpose singleton class for all kinds of meta-logging. May be used to collect metrics or statistics of
 * different implementation parts without much effort for development/experimenting. Is not supposed to be used in
 * production settings.
 *
 * @author Philipp Töws
 *
 */
public class CentralLogger {

	private static CentralLogger instance;

	private final LinkedList<Long> protectedSizes, totalCacheSizes;

	/**
	 * Maps from how many hits happened to how many blocks had this many hits
	 */
	private final Map<Long, Long> inProtectedHits, inCacheHits;

	private long sizesCounter;

	private long protectedSizesAggregator, totalCacheSizesAggregator;

	private static final int AGGREGATION_INTERVAL = 1_000_000;

	private CentralLogger() {
		protectedSizes = new LinkedList<>();
		totalCacheSizes = new LinkedList<>();
		inProtectedHits = new TreeMap<>();
		inCacheHits = new TreeMap<>();
	}

	public static CentralLogger getInstance() {
		if (instance == null) {
			instance = new CentralLogger();
		}
		return instance;
	}

	public void addSizes(long protectedSize, long totalCacheSize) {
		sizesCounter++;
		protectedSizesAggregator += protectedSize;
		totalCacheSizesAggregator += totalCacheSize;
		if ((sizesCounter % AGGREGATION_INTERVAL) == 0) {
			protectedSizes.add(protectedSizesAggregator / AGGREGATION_INTERVAL);
			totalCacheSizes.add(totalCacheSizesAggregator / AGGREGATION_INTERVAL);
			protectedSizesAggregator = 0;
			totalCacheSizesAggregator = 0;
		}
	}

	public void addInProtectedHits(long hits) {
		Long currentHits = inProtectedHits.get(hits);
		if (currentHits == null) {
			currentHits = 0L;
		}
		inProtectedHits.put(hits, currentHits + 1);
	}

	public void addInCacheHits(long hits) {
		Long currentHits = inCacheHits.get(hits);
		if (currentHits == null) {
			currentHits = 0L;
		}
		inCacheHits.put(hits, currentHits + 1);
	}

	public void finish(String configName) {
		System.out.println("===== CentralLogger:");
		System.out.println("===== End CentralLogger");

		CSVFormat csvFileFormat = CSVFormat.RFC4180.withRecordSeparator('\n');
		if (StatisticsDBTest.LOG_SLRU_CACHE_SIZES) {
			try (CSVPrinter csvPrinter = new CSVPrinter(
					new OutputStreamWriter(new FileOutputStream("SLRU-CacheSizesLog_" + configName + ".csv", false),
							"UTF-8"),
					csvFileFormat);) {
				csvPrinter.printRecord("PROTECTED_SIZE", "TOTAL_CACHE_SIZE");
				assert protectedSizes.size() == totalCacheSizes.size();
				while (!protectedSizes.isEmpty()) {
					csvPrinter.printRecord(protectedSizes.removeFirst(), totalCacheSizes.removeFirst());
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		if (StatisticsDBTest.LOG_SLRU_CACHE_HITS) {
			try (CSVPrinter csvPrinter = new CSVPrinter(
					new OutputStreamWriter(new FileOutputStream("SLRU-HitsHistogram_" + configName + ".csv", false),
							"UTF-8"),
					csvFileFormat);) {
				csvPrinter.printRecord("HITS", "IN_PROTECTED", "IN_CACHE");
				Set<Long> allHitNumbers = new TreeSet<>(inCacheHits.keySet());
				allHitNumbers.addAll(inProtectedHits.keySet());
				for (Long hitNumber : allHitNumbers) {
					Long p = inProtectedHits.get(hitNumber);
					if (p == null) {
						p = 0L;
					}
					Long c = inCacheHits.get(hitNumber);
					if (c == null) {
						c = 0L;
					}
					csvPrinter.printRecord(hitNumber, p, c);
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

}
