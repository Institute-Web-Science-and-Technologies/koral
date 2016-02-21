package de.uni_koblenz.west.cidre.common.utils;

/**
 * Converts primitive numerical values into a byte array and back again.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class NumberConversion {

	public static long bytes2long(byte[] bytes) {
		return bytes2long(bytes, 0);
	}

	public static long bytes2long(byte[] bytes, int startIndex) {
		long longValue = 0;
		for (int i = startIndex; i < startIndex + Long.BYTES; i++) {
			longValue = longValue << Byte.SIZE;
			longValue |= (bytes[i] & 0x000000ff);
		}
		return longValue;
	}

	public static int bytes2int(byte[] bytes) {
		return bytes2int(bytes, 0);
	}

	public static int bytes2int(byte[] bytes, int startIndex) {
		int intValue = 0;
		for (int i = startIndex; i < startIndex + Integer.BYTES; i++) {
			intValue = intValue << Byte.SIZE;
			intValue |= (bytes[i] & 0x000000ff);
		}
		return intValue;
	}

	public static short bytes2short(byte[] bytes) {
		return bytes2short(bytes, 0);
	}

	public static short bytes2short(byte[] bytes, int startIndex) {
		short shortValue = 0;
		for (int i = startIndex; i < startIndex + Short.BYTES; i++) {
			shortValue = (short) (shortValue << Byte.SIZE);
			shortValue |= (bytes[i] & 0x000000ff);
		}
		return shortValue;
	}

	public static byte[] long2bytes(long value) {
		byte[] result = new byte[Long.BYTES];
		long2bytes(value, result, 0);
		return result;
	}

	public static void long2bytes(long value, byte[] destinationArray,
			int firstIndex) {
		for (int i = firstIndex + Long.BYTES - 1; i >= firstIndex; i--) {
			destinationArray[i] = (byte) (value & 0x000000ff);
			value = value >>> Byte.SIZE;
		}
	}

	public static byte[] int2bytes(int value) {
		byte[] result = new byte[Integer.BYTES];
		int2bytes(value, result, 0);
		return result;
	}

	public static void int2bytes(int value, byte[] destinationArray,
			int firstIndex) {
		for (int i = firstIndex + Integer.BYTES - 1; i >= firstIndex; i--) {
			destinationArray[i] = (byte) (value & 0x000000ff);
			value = value >>> Byte.SIZE;
		}
	}

	public static byte[] short2bytes(short value) {
		byte[] result = new byte[Short.BYTES];
		short2bytes(value, result, 0);
		return result;
	}

	public static void short2bytes(short value, byte[] destinationArray,
			int firstIndex) {
		for (int i = firstIndex + Short.BYTES - 1; i >= firstIndex; i--) {
			destinationArray[i] = (byte) (value & 0x000000ff);
			value = (short) (value >>> Byte.SIZE);
		}
	}

	public static String id2description(long id) {
		String result = id + "(computer=";
		result += (id >>> (Integer.SIZE + Short.SIZE));
		result += ",query=";
		result += ((id << Short.SIZE) >>> (Short.SIZE + Short.SIZE));
		result += ",task=";
		result += (id & 0x00_00_00_00_00_00_ff_ff);
		result += ")";
		return result;
	}

}
