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
		dnarray = new DynamicNumberArray(SIZE);
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

}
