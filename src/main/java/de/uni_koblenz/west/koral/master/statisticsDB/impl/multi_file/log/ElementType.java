package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log;

/**
 * Stores information about an element in a row layout. Such an element is basically a data type and has attributes like
 * how much space it needs.
 *
 * @author Philipp Töws
 *
 */
public enum ElementType {
	BIT(1L),
	BYTE(Byte.MAX_VALUE, Byte.BYTES),
	SHORT(Short.MAX_VALUE, Short.BYTES),
	INTEGER(Integer.MAX_VALUE, Integer.BYTES),
	LONG(Long.MAX_VALUE, Long.BYTES);

	private boolean hasMaxValue;

	private boolean hasLayoutLength;

	private long maxValue;

	private int layoutLength;

	ElementType(long maxValue, int layoutLength) {
		this.maxValue = maxValue;
		this.layoutLength = layoutLength;
		hasLayoutLength = true;
		hasMaxValue = true;
	}

	private ElementType(long maxValue) {
		this.maxValue = maxValue;
		hasMaxValue = true;
		hasLayoutLength = false;
	}

	private ElementType(int layoutLength) {
		this.layoutLength = layoutLength;
		hasLayoutLength = true;
		hasMaxValue = false;
	}

	private ElementType() {
		hasMaxValue = false;
		hasLayoutLength = false;
	}

	public boolean hasMaxValue() {
		return hasMaxValue;
	}

	public boolean hasLayoutLength() {
		return hasLayoutLength;
	}

	public long getMaxValue() {
		if (!hasMaxValue) {
			throw new UnsupportedOperationException("Element type " + toString() + " does not support a value limit");
		}
		return maxValue;
	}

	public int getLayoutLength() {
		if (!hasLayoutLength) {
			throw new UnsupportedOperationException(
					"Element type " + toString() + " does not support a fixed layout length");
		}
		return layoutLength;
	}
}
