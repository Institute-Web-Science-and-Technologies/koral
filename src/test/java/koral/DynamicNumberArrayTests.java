/**
 *
 */
package koral;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.DynamicNumberArray;

/**
 * @author Philipp Töws
 *
 */
class DynamicNumberArrayTests {

	private DynamicNumberArray dnarray;

	private static final int SIZE = 10;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeEach
	void setUp() throws Exception {
		dnarray = new DynamicNumberArray(SIZE, DynamicNumberArray.DEFAULT_EXTENSION_LENGTH);
	}

	@Test
	void getEmptyValueTest() {
		for (int i = 0; i < SIZE; i++) {
			assertEquals(0, dnarray.get(0));
		}
	}

	@Test
	void setLowValuesTest() {
		for (int i = 0; i < SIZE; i++) {
			dnarray.set(i, i * 2);
		}
		for (int i = 0; i < SIZE; i++) {
			assertEquals(i * 2, dnarray.get(i));
		}
	}

	@Test
	void upgradeMaxTest() {
		dnarray.set(1, 56);
		dnarray.set(0, Byte.MAX_VALUE + 1);
		assertEquals(56, dnarray.get(1));
		assertEquals(Byte.MAX_VALUE + 1, dnarray.get(0));
	}

	@Test
	void upgradeMinTest() {
		dnarray.set(1, 56);
		dnarray.set(0, Byte.MIN_VALUE - 1);
		assertEquals(56, dnarray.get(1));
		assertEquals(Byte.MIN_VALUE - 1, dnarray.get(0));
	}

	@Test
	void upgradeMinusOneTest() {
		dnarray.set(0, 1);
		dnarray.set(2, -1);
		dnarray.set(1, Byte.MAX_VALUE + 1);
		assertEquals(1, dnarray.get(0));
		assertEquals(-1, dnarray.get(2));
		assertEquals(Byte.MAX_VALUE + 1, dnarray.get(1));
	}

	@Test
	void bigUpgradeMaxTest() {
		dnarray.set(1, 56);
		dnarray.set(0, Integer.MAX_VALUE + 1L);
		assertEquals(56, dnarray.get(1));
		assertEquals(Integer.MAX_VALUE + 1L, dnarray.get(0));
	}

	@Test
	void bigUpgradeMinTest() {
		dnarray.set(1, 56);
		dnarray.set(0, Integer.MIN_VALUE - 1L);
		assertEquals(56, dnarray.get(1));
		assertEquals(Integer.MIN_VALUE - 1L, dnarray.get(0));
	}

	@Test
	void moveSmallAtZeroTest() {
		setSmallMoveValues();
		dnarray.move(0, 1, 9);
		assertContent(new long[] { 0, -1, 126, 53, 0, -117, 2, 98, 11, 7 });
	}

	@Test
	void moveSmallAtOneTest() {
		setSmallMoveValues();
		dnarray.move(1, 3, 7);
		assertContent(new long[] { -1, 0, 0, 126, 53, 0, -117, 2, 98, 11 });
	}

	@Test
	void moveSmallToLeftTest() {
		setSmallMoveValues();
		dnarray.move(3, 1, 7);
		assertContent(new long[] { -1, 0, -117, 2, 98, 11, 7, 127, 0, 0 });
	}

	@Test
	void moveBigAtOneTest() {
		setBigMoveValues();
		dnarray.move(1, 3, 7);
		assertContent(new long[] { -1, 0, 0, 65534, 11252, 0, -26317, 2, 980, 112 });
	}

	@Test
	void moveBigToLeftTest() {
		setBigMoveValues();
		dnarray.move(3, 1, 7);
		assertContent(new long[] { -1, 0, -26317, 2, 980, 112, 55123, 65535, 0, 0 });
	}

	private void setSmallMoveValues() {
		setValueArray(new long[] {
				-1, 126, 53, 0, -117, 2, 98, 11, 7, 127
		});
	}

	private void setBigMoveValues() {
		setValueArray(new long[] {
				-1, 65534, 11252, 0, -26317, 2, 980, 112, 55123, 65535
		});
		assertEquals(-1, dnarray.get(0));
	}

	private void setValueArray(long[] array) {
		for (int i = 0; i < array.length; i++) {
			dnarray.set(i, array[i]);
		}
	}

	private void assertContent(long[] array) {
		for (int i = 0; i < array.length; i++) {
			assertEquals(array[i], dnarray.get(i));
		}
	}

}
