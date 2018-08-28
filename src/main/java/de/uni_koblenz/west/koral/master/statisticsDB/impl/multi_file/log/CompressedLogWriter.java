package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.GZIPOutputStream;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.Utils;

public class CompressedLogWriter {

	private final Map<Integer, Map<String, ElementType>> rowLayouts;

	private final Map<Integer, Integer> rowLengths;

	private final File storageFile;

	private OutputStream out;

	private boolean closed;

	private final Map<Integer, Long> rowTypeOccurences;

	public CompressedLogWriter(File storageFile, Map<Integer, Map<String, ElementType>> rowLayouts) {
		this.storageFile = storageFile;
		this.rowLayouts = rowLayouts;
		rowTypeOccurences = new HashMap<>();
		rowLengths = new HashMap<>();
		for (Entry<Integer, Map<String, ElementType>> entry : rowLayouts.entrySet()) {
			rowLengths.put(entry.getKey(), calculateLayoutLength(entry.getValue()));
			rowTypeOccurences.put(entry.getKey(), 0L);
		}
		open();
	}

	public void open() {
		boolean fileExists = storageFile.exists();
		try {
			out = new BufferedOutputStream(new GZIPOutputStream(new FileOutputStream(storageFile, true)));
//			out = new BufferedOutputStream(new FileOutputStream(storageFile, true));
//			out = new FileOutputStream(storageFile, true);
			closed = false;
			if (!fileExists) {
				writeHeader();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void writeHeader() throws IOException {
		// Header consists of:
		// Integer Amount of rowTypes, rowTypes
		// rowTypes: Integer rowType(ID), Integer Amount of element tuples, element tuples
		// element tuples: Integer String length, Bytes String element name, Integer ElementType
		// Example (Parantheses for clarification):
		// 2 [0 4 ({1 "a" 1} {1 "b" 2} {1 "c" 1} {1 "d" 0})] [1 3 ({3 "foo" 5} {3 "bar" 2} {3 "baz" 5})]
		out.write(int2byteArray(rowLayouts.size()));
		for (Entry<Integer, Map<String, ElementType>> entry : rowLayouts.entrySet()) {
			out.write(int2byteArray(entry.getKey()));
			Map<String, ElementType> elements = entry.getValue();
			out.write(int2byteArray(elements.size()));
			for (Entry<String, ElementType> elementEntry : elements.entrySet()) {
				byte[] elementName = elementEntry.getKey().getBytes(StandardCharsets.UTF_8);
				out.write(int2byteArray(elementName.length));
				out.write(elementName);
				out.write(int2byteArray(elementEntry.getValue().ordinal()));
			}
		}
	}

	public void log(int rowType, Map<String, Object> data) {
		long occurences = rowTypeOccurences.get(rowType);
		rowTypeOccurences.put(rowType, occurences + 1);

		write(int2byteArray(rowType));
		Map<String, ElementType> rowLayout = rowLayouts.get(rowType);
		byte[] row = new byte[rowLengths.get(rowType)];
		int cursor = 0;
		LinkedList<Byte> bits = new LinkedList<>();
		for (String elementName : rowLayout.keySet()) {
			Object value = data.get(elementName);
			ElementType elementType = rowLayout.get(elementName);
			if (elementType == ElementType.BIT) {
				bits.add(object2byte(value));
			} else {
				cursor = addElement(row, cursor, elementType, value);
			}
		}
		Iterator<Byte> bitIterator = bits.iterator();

		while (bitIterator.hasNext()) {
			byte bitGroup = 0;
			for (int bitCursor = 0; (bitCursor < Byte.SIZE) && bitIterator.hasNext(); bitCursor++) {
				bitGroup |= bitIterator.next() << (Byte.SIZE - bitCursor - 1);
			}
			row[cursor] = bitGroup;
			cursor++;
		}
		write(row);

	}

	private int addElement(byte[] row, int cursor, ElementType elementType, Object value) {
		if (elementType == null) {
			throw new NullPointerException();
		}
		if (elementType == ElementType.BIT) {
			throw new AssertionError("Bits are supposed to be added separately");
		}
		if (elementType == ElementType.BYTE) {
			row[cursor] = object2byte(value);
			return cursor + 1;
		}
		if (elementType == ElementType.INTEGER) {
			int integer = (int) value;
			Utils.writeLongIntoBytes(integer, row, cursor, Integer.BYTES);
			return cursor + Integer.BYTES;
		}
		if (elementType == ElementType.SHORT) {
			int shortValue = (short) value;
			Utils.writeLongIntoBytes(shortValue, row, cursor, Short.BYTES);
			return cursor + Short.BYTES;
		}
		throw new IllegalArgumentException("Unknown value for elementType: " + elementType);
	}

	static Integer calculateLayoutLength(Map<String, ElementType> layout) {
		int layoutLength = 0;
		int bitCount = 0;
		for (ElementType elementType : layout.values()) {
			if (elementType == ElementType.BIT) {
				bitCount++;
			} else if (elementType == ElementType.BYTE) {
				layoutLength += 1;
			} else if (elementType == ElementType.INTEGER) {
				layoutLength += Integer.BYTES;
			} else if (elementType == ElementType.SHORT) {
				layoutLength += Short.BYTES;
			}
		}
		layoutLength += bitCount / 8;
		if ((bitCount % 8) > 0) {
			layoutLength++;
		}
		return layoutLength;
	}

	private void write(byte[] row) {
		// TODO: Is it okay to reopen every time although it was closed?
		if (closed) {
			open();
		}
		try {
			out.write(row);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void close() {
		if (!closed) {
			try {
				out.flush();
				out.close();
				closed = true;
			} catch (IOException e) {
				e.printStackTrace();
			}
			if (rowTypeOccurences.values().stream().anyMatch(occ -> occ > 0)) {
				System.out.println("Row Type Occurences:");
				for (Entry<Integer, Long> entry : rowTypeOccurences.entrySet()) {
					System.out.println("Type " + entry.getKey() + ": " + String.format("%,d", entry.getValue()));
					rowTypeOccurences.put(entry.getKey(), 0L);
				}
			}
		}
	}

	private static byte[] int2byteArray(int number) {
		byte[] array = new byte[Integer.BYTES];
		for (int i = 0; i < Integer.BYTES; i++) {
			array[i] = (byte) (number >>> ((Integer.BYTES - i - 1) * Byte.SIZE));
		}
		return array;
	}

	private static byte object2byte(Object value) {
		if (value instanceof Boolean) {
			return (byte) (((boolean) value) ? 1 : 0);
		} else if (value instanceof Integer) {
			return ((Integer) value).byteValue();
		} else {
			return (Byte) value;
		}
	}
}
