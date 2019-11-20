package koral;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.Utils;

class UtilsTests {

	@Test
	void neededBytesForValueZeroTest() {
		assertEquals(1, Utils.neededBytesForValue(0, true));
		assertEquals(1, Utils.neededBytesForValue(0, false));
	}

	@Test
	void neededBytesForValueMinusOneTest() {
		assertEquals(1, Utils.neededBytesForValue(-1, true));
		assertEquals(8, Utils.neededBytesForValue(-1, false));
	}

	@Test
	void neededBytesForValueByteMaxTest() {
		assertEquals(1, Utils.neededBytesForValue(127, true));
		assertEquals(1, Utils.neededBytesForValue(127, false));
	}

	@Test
	void neededBytesForValueByteMaxPlusOneTest() {
		assertEquals(2, Utils.neededBytesForValue(128, true));
		assertEquals(1, Utils.neededBytesForValue(128, false));
	}

	@Test
	void neededBytesForValue8BitMaxTest() {
		assertEquals(2, Utils.neededBytesForValue(255, true));
		assertEquals(1, Utils.neededBytesForValue(255, false));
	}

	@Test
	void neededBytesForValueByteMinTest() {
		assertEquals(1, Utils.neededBytesForValue(-128, true));
		assertEquals(8, Utils.neededBytesForValue(-128, false));
	}

	@Test
	void neededBytesForValueByteMinMinusOneTest() {
		assertEquals(2, Utils.neededBytesForValue(-129, true));
		assertEquals(8, Utils.neededBytesForValue(-129, false));
	}

}
