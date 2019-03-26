package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.LinkedList;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

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

	private long sizesCounter;

	private long protectedSizesAggregator, totalCacheSizesAggregator;

	private static final int AGGREGATION_INTERVAL = 1_000_000;

	private CentralLogger() {
		protectedSizes = new LinkedList<>();
		totalCacheSizes = new LinkedList<>();
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

	public void finish() {
		System.out.println("===== CentralLogger:");
		System.out.println("===== End CentralLogger");

		CSVFormat csvFileFormat = CSVFormat.RFC4180.withRecordSeparator('\n');
		try (CSVPrinter csvPrinter = new CSVPrinter(
				new OutputStreamWriter(new FileOutputStream("SLRU-CacheSizesLog.csv", false), "UTF-8"),
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

}
