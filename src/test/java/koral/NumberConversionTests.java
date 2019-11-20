/**
 *
 */
package koral;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import de.uni_koblenz.west.koral.common.utils.NumberConversion;

/**
 * @author Philipp Töws
 *
 */
class NumberConversionTests {

	@Test
	void signedBytes2longByteMinTest() {
		byte[] n = new byte[] { -1, -128 };
		assertEquals(-128L, NumberConversion.signedBytes2long(n, 0, 2));
	}

	@Test
	void signedBytes2longByteMinMinusOneTest() {
		byte[] n = new byte[] { -1, 127 };
		assertEquals(-129L, NumberConversion.signedBytes2long(n, 0, 2));
	}

	@Test
	void signedBytes2longByteMaxTest() {
		byte[] n = new byte[] { 0, 127 };
		assertEquals(127L, NumberConversion.signedBytes2long(n, 0, 2));
	}

	@Test
	void signedBytes2longByteMaxPlusOneTest() {
		byte[] n = new byte[] { 0, -128 };
		assertEquals(128L, NumberConversion.signedBytes2long(n, 0, 2));
	}

	@Test
	void signedBytes2long8BitMaxTest() {
		byte[] n = new byte[] { 0, -1 };
		assertEquals(255L, NumberConversion.signedBytes2long(n, 0, 2));
	}

}
