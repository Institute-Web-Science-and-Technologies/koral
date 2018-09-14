package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;

import de.uni_koblenz.west.koral.common.utils.NumberConversion;

public class CompressedLogReader {

	private final File storageFile;

	private InputStream input;

	private final Map<Integer, Map<String, ElementType>> rowLayouts;

	private final Map<Integer, Integer> rowLengths;

	private final LinkedList<StorageLogReadListener> listeners;

	private final HashMap<StorageLogReadListener, Long> listenerComputationTimes;

	public CompressedLogReader(File storageFile) {
		this.storageFile = storageFile;
		rowLayouts = new HashMap<>();
		rowLengths = new HashMap<>();
		listeners = new LinkedList<>();
		listenerComputationTimes = new HashMap<>();
		open();
		try {
			readHeader();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void open() {
		try {
			input = new BufferedInputStream(new GZIPInputStream(new FileInputStream(storageFile)));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void registerListener(StorageLogReadListener listener) {
		listeners.add(listener);
	}

	/**
	 * Reads the whole log while notifying all registered listeners ({@link #registerListener(StorageLogReadListener)})
	 * about the current row, after each row read.
	 */
	public void readAll() {
		LogRow logRow;
		while ((logRow = readRow()) != null) {
			for (StorageLogReadListener listener : listeners) {
				long start = System.nanoTime();
				listener.onLogRowRead(logRow.getRowType(), logRow.getData());
				long time = System.nanoTime() - start;
				Long currentComputationTime = listenerComputationTimes.get(listener);
				if (currentComputationTime == null) {
					currentComputationTime = 0L;
				}
				listenerComputationTimes.put(listener, currentComputationTime + time);
			}
		}
	}

	private void readHeader() throws IOException {
		int rowTypeCount = readInt();
		for (int rowTypeCounter = 0; rowTypeCounter < rowTypeCount; rowTypeCounter++) {
			Map<String, ElementType> rowLayout = new TreeMap<>();
			int rowType = readInt();
			int elementCount = readInt();
			for (int elementCounter = 0; elementCounter < elementCount; elementCounter++) {
				int elementNameLength = readInt();
				byte[] elementNameBytes = new byte[elementNameLength];
				input.read(elementNameBytes);
				String elementName = new String(elementNameBytes, StandardCharsets.UTF_8);
				int elementTypeOrdinal = readInt();
				ElementType elementType = ElementType.values()[elementTypeOrdinal];
				// TODO: rowLayout elements should be sorted like the header data
				// Using a TreeMap on strings sorts by the string key, it should be sorted by insertion order
				// to guarantee correct interpretation of the raw body data
				rowLayout.put(elementName, elementType);
			}
			rowLayouts.put(rowType, rowLayout);
			rowLengths.put(rowType, CompressedLogWriter.calculateLayoutLength(rowLayout));
		}
	}

	/**
	 * Returns a singleton instance of {@link LogRow} that contains the information of the next row in the log.
	 *
	 * @return null if no row is left
	 */
	public LogRow readRow() {
		try {
			int rowType = readInt();
			int rowLength = rowLengths.get(rowType);
			byte[] row = new byte[rowLength];
			if (input.read(row) == -1) {
				throw new RuntimeException("Invalid log format, row not complete");
			}
			return LogRow.getInstance(rowType, deserializeRow(rowType, row));
		} catch (EOFException e) {
			// No row left
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return null;
	}

	private Map<String, Object> deserializeRow(int rowType, byte[] row) {
		Map<String, Object> data = new HashMap<>();
		int cursor = 0;
		Map<String, ElementType> layout = rowLayouts.get(rowType);
		LinkedList<String> elementNamesOfBits = new LinkedList<>();
		// The layout entries are sorted like the binary data, so this order must be used
		for (Entry<String, ElementType> entry : layout.entrySet()) {
			String elementName = entry.getKey();
			ElementType elementType = entry.getValue();
			if (elementType == null) {
				throw new NullPointerException();
			} else if (elementType == ElementType.BIT) {
				elementNamesOfBits.add(elementName);
			} else if (elementType == ElementType.BYTE) {
				data.put(elementName, row[cursor]);
				cursor++;
			} else if (elementType == ElementType.INTEGER) {
				data.put(elementName, NumberConversion.bytes2int(row, cursor));
				cursor += Integer.BYTES;
			} else if (elementType == ElementType.SHORT) {
				data.put(elementName, NumberConversion.bytes2short(row, cursor));
				cursor += Short.BYTES;
			} else if (elementType == ElementType.LONG) {
				data.put(elementName, NumberConversion.bytes2long(row, cursor));
				cursor += Short.BYTES;
			} else {
				throw new IllegalArgumentException("Unkown element type: " + elementType);
			}
		}
		for (; cursor < row.length; cursor++) {
			for (int bitCursor = 0; (bitCursor < Byte.SIZE) && !elementNamesOfBits.isEmpty(); bitCursor++) {
				byte filteredBitGroup = (byte) (row[cursor] & (1 << (Byte.SIZE - bitCursor - 1)));
				// Move the bit to the last position
				byte value = (byte) ((0xFF & filteredBitGroup) >>> (Byte.SIZE - bitCursor - 1));
				data.put(elementNamesOfBits.removeFirst(), value);
			}
		}
		return data;
	}

	private int readInt() throws IOException {
		byte[] array = new byte[Integer.BYTES];
		if (input.read(array) == -1) {
			throw new EOFException();
		}
		return NumberConversion.bytes2int(array);
	}

	public Map<StorageLogReadListener, Long> getListenerComputationTimes() {
		return listenerComputationTimes;
	}

	public void close() {
		try {
			input.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
