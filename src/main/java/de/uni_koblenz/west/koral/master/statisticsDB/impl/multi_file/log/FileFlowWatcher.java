package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

/**
 * Listens to file switch events (when a row migrates to a different extra file) and generates a CSV based on this.
 *
 * @author Philipp Töws
 *
 */
public class FileFlowWatcher {

	private static FileFlowWatcher instance;

	private final String outputDir;

	private final HashMap<SwitchReason, HashMap<DirectedEdge, Long>> switchMaps;

	public static enum SwitchReason {
		NEW_COLUMN,
		VALUE_SIZE_INCREASE
	}

	private FileFlowWatcher(String outputDir) {
		this.outputDir = outputDir;
		switchMaps = new HashMap<>();
		for (SwitchReason reason : SwitchReason.values()) {
			switchMaps.put(reason, new HashMap<>());
		}
	}

	public static FileFlowWatcher createInstance(String outputDir) {
		if (instance != null) {
			throw new IllegalStateException("FileFlowWatcher instance is already created");
		}
		instance = new FileFlowWatcher(outputDir);
		return instance;
	}

	public static FileFlowWatcher getInstance() {
		return instance;
	}

	public void notify(int fileFrom, int fileTo, SwitchReason reason) {
		HashMap<DirectedEdge, Long> switchMap = switchMaps.get(reason);
		DirectedEdge directedEdge = new DirectedEdge(fileFrom, fileTo);
		Long count = switchMap.get(directedEdge);
		if (count == null) {
			count = 0L;
		}
		switchMap.put(directedEdge, ++count);
	}

	private void writeToFile() {
		for (SwitchReason reason : SwitchReason.values()) {
			HashMap<DirectedEdge, Long> switchMap = switchMaps.get(reason);
			File csvFile = new File(outputDir, "fileFlow_" + reason.toString() + ".csv.gz");
			CSVFormat csvFileFormat = CSVFormat.RFC4180.withRecordSeparator('\n');
			try (CSVPrinter csvPrinter = new CSVPrinter(
					new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(csvFile, false)), "UTF-8"),
					csvFileFormat)) {
				csvPrinter.printRecord("FILE_FROM", "FILE_TO", "COUNT");
				for (Entry<DirectedEdge, Long> entry : switchMap.entrySet()) {
					csvPrinter.printRecord(entry.getKey().nodeFrom, entry.getKey().nodeTo, entry.getValue());
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 * Writes collected data to files.
	 */
	public void close() {
		writeToFile();
	}

	class DirectedEdge {
		final int nodeFrom, nodeTo;

		public DirectedEdge(int nodeFrom, int nodeTo) {
			this.nodeFrom = nodeFrom;
			this.nodeTo = nodeTo;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if ((o == null) || (getClass() != o.getClass())) {
				return false;
			}
			DirectedEdge otherEdge = (DirectedEdge) o;
			if ((nodeFrom == otherEdge.nodeFrom) && (nodeTo == otherEdge.nodeTo)) {
				return true;
			} else {
				return false;
			}
		}

		@Override
		public int hashCode() {
			return Objects.hash(nodeFrom, nodeTo);
		}
	}
}
