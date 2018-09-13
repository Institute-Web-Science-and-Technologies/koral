package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.log;

public enum ElementType {
	BIT(1L),
	BYTE(Byte.MAX_VALUE, Byte.BYTES),
	SHORT(Short.MAX_VALUE, Short.BYTES),
	INTEGER(Integer.MAX_VALUE, Integer.BYTES),
	LONG(Long.MAX_VALUE, Long.BYTES);

	private boolean noLayoutLength;

	private boolean noMaxValue;

	private long maxValue;

	private int layoutLength;

	ElementType(long maxValue, int layoutLength) {
		this.maxValue = maxValue;
		this.layoutLength = layoutLength;
	}

	private ElementType(long maxValue) {
		this.maxValue = maxValue;
		noLayoutLength = true;
	}

	private ElementType(int layoutLength) {
		this.layoutLength = layoutLength;
		noMaxValue = true;
	}

	private ElementType() {
		noLayoutLength = true;
		noMaxValue = true;
	}

	public long getMaxValue() {
		if (noMaxValue) {
			throw new UnsupportedOperationException("Element type " + toString() + " does not support a value limit");
		}
		return maxValue;
	}

	public int getLayoutLength() {
		if (noLayoutLength) {
			throw new UnsupportedOperationException(
					"Element type " + toString() + " does not support a fixed layout length");
		}
		return layoutLength;
	}
}
