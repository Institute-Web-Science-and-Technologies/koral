/**
 *
 */
package koral;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.DynamicNumberArray;

/**
 * @author Philipp Töws
 *
 */
class DynamicNumberArrayTests {

	private DynamicNumberArray dna;

	private static final int SIZE = 10;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeEach
	void setUp() throws Exception {
		dna = new DynamicNumberArray(SIZE, 1, DynamicNumberArray.DEFAULT_EXTENSION_LENGTH);
	}

	@Test
	void setupTest() {
		assertEquals(SIZE, dna.capacity());
		assertEquals(-1, dna.getLastUsedIndex());
	}

	@Test
	void loadExistingSmallArrayTest() {
		byte[] a = { 0, -1, 126, 53, 0, -117, 0, 0 };
		DynamicNumberArray newDna = new DynamicNumberArray(a, 1, 10);
		assertEquals(8, newDna.capacity());
		assertEquals(5, newDna.getLastUsedIndex());
		assertContent(ArrayUtils.toObject(a), newDna);
	}

	@Test
	void loadExistingBigArrayTest() {
		byte[] a = { -1, -1, -1, 0, 0, 0, -1, -103, 51, 0, 0, 2, 0, 3, -44, 0, 0, 112, 0, -41, 83, 0, -1, -1, 0, 0, 0 };
		DynamicNumberArray newDna = new DynamicNumberArray(a, 3, 10);
		assertEquals(9, newDna.capacity());
		assertEquals(7, newDna.getLastUsedIndex());
		assertContent(new Number[] { -1, 0, -26317, 2, 980, 112, 55123, 65535, 0 }, newDna);
	}

	@Test
	void loadExistingNullArrayTest() {
		assertThrows(IllegalArgumentException.class, () -> new DynamicNumberArray(null, 1, 10));
	}

	@Test
	void getEmptyValueTest() {
		for (int i = 0; i < SIZE; i++) {
			assertEquals(0, dna.get(0));
		}

		assertEquals(-1, dna.getLastUsedIndex());
	}

	@Test
	void resetTest() {
		setBigValues();
		// Provoke extension
		dna.set(14, 53278, null);
		dna.reset();
		assertEquals(SIZE, dna.capacity());
		assertEquals(-1, dna.getLastUsedIndex());
		Number[] emptyArray = new Number[SIZE];
		Arrays.fill(emptyArray, 0);
		assertContent(emptyArray);
	}

	@Test
	void setLowValuesTest() {
		for (int i = 0; i < SIZE; i++) {
			dna.set(i, (i + 1) * 2, null);
			assertEquals(i, dna.getLastUsedIndex());
		}
		for (int i = 0; i < SIZE; i++) {
			assertEquals((i + 1) * 2, dna.get(i));
			assertEquals(SIZE - 1, dna.getLastUsedIndex());
		}
	}

	@Test
	void incSmallTest() {
		setSmallValues();
		dna.inc(2, null);
		assertContent(new Number[] { -1, 126, 54, 0, -117, 2, 98, 11, 7, 127 });
	}

	@Test
	void incBigTest() {
		setBigValues();
		dna.inc(2, null);
		assertContent(new Number[] { -1, 65534, 11253, 0, -26317, 2, 980, 112, 55123, 65535 });
	}

	@Test
	void decSmallTest() {
		setSmallValues();
		dna.dec(2, null);
		assertContent(new Number[] { -1, 126, 52, 0, -117, 2, 98, 11, 7, 127 });
	}

	@Test
	void decBigTest() {
		setBigValues();
		dna.dec(2, null);
		assertContent(new Number[] { -1, 65534, 11251, 0, -26317, 2, 980, 112, 55123, 65535 });
	}

	@Test
	void setLastFieldToZeroTest() {
		setSmallValues();
		dna.set(9, 0, null);
		assertEquals(10, dna.capacity());
		assertEquals(8, dna.getLastUsedIndex());
	}

	@Test
	void setLastFieldToZeroNonConsecutiveTest() {
		setSmallValues();
		dna.set(8, 0, null);
		assertEquals(9, dna.getLastUsedIndex());
		dna.set(9, 0, null);
		// Although value at 8 is = 0
		assertEquals(8, dna.getLastUsedIndex());
		assertEquals(10, dna.capacity());
	}

	@Test
	void upgradeMaxTest() {
		dna.set(1, 56, null);
		dna.set(0, Byte.MAX_VALUE + 1, null);
		assertEquals(56, dna.get(1));
		assertEquals(Byte.MAX_VALUE + 1, dna.get(0));

		assertEquals(1, dna.getLastUsedIndex());
	}

	@Test
	void upgradeMinTest() {
		dna.set(1, 56, null);
		dna.set(0, Byte.MIN_VALUE - 1, null);
		assertEquals(56, dna.get(1));
		assertEquals(Byte.MIN_VALUE - 1, dna.get(0));

		assertEquals(1, dna.getLastUsedIndex());
	}

	@Test
	void upgradeMinusOneTest() {
		dna.set(0, 1, null);
		dna.set(2, -1, null);
		dna.set(1, Byte.MAX_VALUE + 1, null);
		assertEquals(1, dna.get(0));
		assertEquals(-1, dna.get(2));
		assertEquals(Byte.MAX_VALUE + 1, dna.get(1));

		assertEquals(2, dna.getLastUsedIndex());
	}

	@Test
	void bigUpgradeMaxTest() {
		dna.set(1, 56, null);
		dna.set(0, Integer.MAX_VALUE + 1L, null);
		assertEquals(56, dna.get(1));
		assertEquals(Integer.MAX_VALUE + 1L, dna.get(0));

		assertEquals(1, dna.getLastUsedIndex());
	}

	@Test
	void bigUpgradeMinTest() {
		dna.set(1, 56, null);
		dna.set(0, Integer.MIN_VALUE - 1L, null);
		assertEquals(56, dna.get(1));
		assertEquals(Integer.MIN_VALUE - 1L, dna.get(0));

		assertEquals(1, dna.getLastUsedIndex());
	}

	@Test
	void moveNothing() {
		setSmallValues();
		dna.move(0, 0, null);
		assertContent(new Number[] { -1, 126, 53, 0, -117, 2, 98, 11, 7, 127 });
		assertEquals(10, dna.capacity());
		assertEquals(9, dna.getLastUsedIndex());
	}

	@Test
	void moveSmallAtZeroTest() {
		setSmallValues();
		dna.move(0, 1, null);
		assertContent(new Number[] { 0, -1, 126, 53, 0, -117, 2, 98, 11, 7 });
	}

	@Test
	void moveSmallAtOneTest() {
		setSmallValues();
		dna.move(1, 3, null);
		assertContent(new Number[] { -1, 0, 0, 126, 53, 0, -117, 2, 98, 11 });
	}

	@Test
	void moveSmallToRightTest() {
		setSmallValues();
		dna.set(10, 0, null);
		dna.set(11, 0, null);
		dna.set(12, 0, null);
		assertEquals(9, dna.getLastUsedIndex());
		dna.move(1, 3, null);
		assertContent(new Number[] { -1, 0, 0, 126, 53, 0, -117, 2, 98, 11, 7, 127, 0 });
		assertEquals(11, dna.getLastUsedIndex());
	}

	@Test
	void moveSmallToRightWithoutOverlapTest() {
		setSmallValues();
		dna.set(10, 0, null);
		dna.set(11, 0, null);
		dna.set(12, 0, null);
		assertEquals(9, dna.getLastUsedIndex());
		dna.move(1, 10, null);
		assertContent(new Number[] { -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 126, 53, 0, -117, 2, 98, 11, 7, 127 });
		assertEquals(18, dna.getLastUsedIndex());
	}

	@Test
	void moveSmallToLeftTest() {
		setSmallValues();
		assertEquals(SIZE - 1, dna.getLastUsedIndex());
		dna.move(3, 1, null);
		assertContent(new Number[] { -1, 0, -117, 2, 98, 11, 7, 127, 0, 0 });
		assertEquals(SIZE - 3, dna.getLastUsedIndex());
	}

	@Test
	void moveSmallToLeftWithoutOverlapTest() {
		setSmallValues();
		dna.move(9, 1, null);
		assertContent(new Number[] { -1, 127, 53, 0, -117, 2, 98, 11, 7, 0 });
		assertEquals(8, dna.getLastUsedIndex());
	}

	@Test
	void moveBigAtOneTest() {
		setBigValues();
		dna.move(1, 3, null);
		assertContent(new Number[] { -1, 0, 0, 65534, 11252, 0, -26317, 2, 980, 112 });
	}

	@Test
	void moveBigToRightTest() {
		setBigValues();
		dna.set(10, 0, null);
		dna.set(11, 0, null);
		dna.set(12, 0, null);
		assertEquals(9, dna.getLastUsedIndex());
		dna.move(1, 3, null);
		assertContent(new Number[] { -1, 0, 0, 65534, 11252, 0, -26317, 2, 980, 112, 55123, 65535, 0 });
		assertEquals(11, dna.getLastUsedIndex());
	}

	@Test
	void moveBigToLeftTest() {
		setBigValues();
		assertEquals(SIZE - 1, dna.getLastUsedIndex());
		dna.move(3, 1, null);
		assertContent(new Number[] { -1, 0, -26317, 2, 980, 112, 55123, 65535, 0, 0 });
		assertEquals(SIZE - 3, dna.getLastUsedIndex());
	}

	@Test
	void insertGapWithoutExtensionTest() {
		dna.set(0, 127, null);
		dna.set(1, 126, null);
		dna.insertGap(1, 4, null);
		assertEquals(127L, dna.get(0));
		for (int i = 1; i < 5; i++) {
			assertEquals(0L, dna.get(i));
		}
		assertEquals(126L, dna.get(5));
		assertEquals(5, dna.getLastUsedIndex());
	}

	@Test
	void insertGapLeftSmallTest() {
		setSmallValues();
		dna.insertGap(0, 3, null);
		assertContent(new Number[] { 0, 0, 0, -1, 126, 53, 0, -117, 2, 98, 11, 7, 127 });
		assertEquals(12, dna.getLastUsedIndex());
	}

	@Test
	void insertGapMiddleSmallTest() {
		setSmallValues();
		dna.insertGap(2, 3, null);
		assertContent(new Number[] { -1, 126, 0, 0, 0, 53, 0, -117, 2, 98, 11, 7, 127 });
		assertEquals(12, dna.getLastUsedIndex());
	}

	@Test
	void insertGapRightSmallTest() {
		setSmallValues();
		dna.insertGap(9, 3, null);
		assertContent(new Number[] { -1, 126, 53, 0, -117, 2, 98, 11, 7, 0, 0, 0, 127 });
		assertEquals(12, dna.getLastUsedIndex());
	}

	@Test
	void insertGapLeftBigTest() {
		setBigValues();
		dna.insertGap(0, 3, null);
		assertContent(new Number[] { 0, 0, 0, -1, 65534, 11252, 0, -26317, 2, 980, 112, 55123, 65535 });
		assertEquals(12, dna.getLastUsedIndex());
	}

	@Test
	void insertGapMiddleBigTest() {
		setBigValues();
		dna.insertGap(2, 3, null);
		assertContent(new Number[] { -1, 65534, 0, 0, 0, 11252, 0, -26317, 2, 980, 112, 55123, 65535 });
		assertEquals(12, dna.getLastUsedIndex());
	}

	@Test
	void insertGapRightBigTest() {
		setBigValues();
		dna.insertGap(9, 3, null);
		assertContent(new Number[] { -1, 65534, 11252, 0, -26317, 2, 980, 112, 55123, 0, 0, 0, 65535 });
		assertEquals(12, dna.getLastUsedIndex());
	}

	@Test
	void insertBigGapBigTest() {
		setBigValues();
		dna.insertGap(7, 3000, null);
		long[] leftPart = new long[] { -1, 65534, 11252, 0, -26317, 2, 980 };
		long[] rightPart = new long[] { 112, 55123, 65535 };
		for (int i = 0; i < 7; i++) {
			assertEquals(leftPart[i], dna.get(i));
		}
		for (int i = 7; i < 3007; i++) {
			assertEquals(0L, dna.get(i));
		}
		for (int i = 3007; i < 3010; i++) {
			assertEquals(rightPart[i - 3007], dna.get(i));
		}
		assertEquals(3009, dna.getLastUsedIndex());
	}

	///////////////
	// Helpers
	///////////////

	private void setSmallValues() {
		setValueArray(new long[] {
				-1, 126, 53, 0, -117, 2, 98, 11, 7, 127
		});
	}

	private void setBigValues() {
		setValueArray(new long[] {
				-1, 65534, 11252, 0, -26317, 2, 980, 112, 55123, 65535
		});
		assertEquals(-1, dna.get(0));
	}

	private void setValueArray(long[] array) {
		for (int i = 0; i < array.length; i++) {
			dna.set(i, array[i], null);
		}
	}

	private void assertContent(Number[] array) {
		assertContent(array, dna);
	}

	private void assertContent(Number[] array, DynamicNumberArray dna) {
		for (int i = 0; i < array.length; i++) {
			assertEquals(array[i].longValue(), dna.get(i));
		}
	}

}
