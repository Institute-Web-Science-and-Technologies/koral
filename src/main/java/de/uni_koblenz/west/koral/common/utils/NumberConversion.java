/*
 * This file is part of Koral.
 *
 * Koral is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Koral is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Leser General Public License
 * along with Koral.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright 2016 Daniel Janke
 */
package de.uni_koblenz.west.koral.common.utils;

/**
 * Converts primitive numerical values into a byte array and back again.
 *
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class NumberConversion {

	public static long bytes2long(byte[] bytes) {
		return NumberConversion.bytes2long(bytes, 0);
	}

	public static long bytes2long(byte[] bytes, int startIndex) {
		return bytes2long(bytes, startIndex, Long.BYTES);
	}

	public static long bytes2long(byte[] bytes, int startIndex, int length) {
		long longValue = 0;
		for (int i = startIndex; i < (startIndex + length); i++) {
			longValue = longValue << Byte.SIZE;
			longValue |= (bytes[i] & 0x000000ff);
		}
		return longValue;
	}

	public static long signedBytes2long(byte[] bytes, int startIndex, int length) {
		long longValue = NumberConversion.bytes2long(bytes, startIndex, length);
		if ((longValue & (1L << ((length * Byte.SIZE) - 1))) != 0) {
			// Number should be negative, set leading zeros to one
			longValue |= -1L ^ ((1L << (length * Byte.SIZE)) - 1);
		}
		return longValue;
	}

	public static int bytes2int(byte[] bytes) {
		return NumberConversion.bytes2int(bytes, 0);
	}

	public static int bytes2int(byte[] bytes, int startIndex) {
		int intValue = 0;
		for (int i = startIndex; i < (startIndex + Integer.BYTES); i++) {
			intValue = intValue << Byte.SIZE;
			intValue |= (bytes[i] & 0x000000ff);
		}
		return intValue;
	}

	public static short bytes2short(byte[] bytes) {
		return NumberConversion.bytes2short(bytes, 0);
	}

	public static short bytes2short(byte[] bytes, int startIndex) {
		short shortValue = 0;
		for (int i = startIndex; i < (startIndex + Short.BYTES); i++) {
			shortValue = (short) (shortValue << Byte.SIZE);
			shortValue |= (bytes[i] & 0x000000ff);
		}
		return shortValue;
	}

	public static byte[] long2bytes(long value) {
		byte[] result = new byte[Long.BYTES];
		NumberConversion.long2bytes(value, result, 0);
		return result;
	}

	public static void long2bytes(long value, byte[] destinationArray, int firstIndex) {
		NumberConversion.long2bytes(value, destinationArray, firstIndex, Long.BYTES);
	}

	public static void long2bytes(long value, byte[] destinationArray, int firstIndex, int length) {
		for (int i = (firstIndex + length) - 1; i >= firstIndex; i--) {
			destinationArray[i] = (byte) (value & 0x000000ff);
			value = value >>> Byte.SIZE;
		}
	}

	public static void signedLong2bytes(long value, byte[] destinationArray, int firstIndex, int length) {
		for (int i = (firstIndex + length) - 1; i >= firstIndex; i--) {
			destinationArray[i] = (byte) (value & 0x000000ff);
			value = value >> Byte.SIZE;
		}
	}

	public static byte[] int2bytes(int value) {
		byte[] result = new byte[Integer.BYTES];
		NumberConversion.int2bytes(value, result, 0);
		return result;
	}

	public static void int2bytes(int value, byte[] destinationArray, int firstIndex) {
		for (int i = (firstIndex + Integer.BYTES) - 1; i >= firstIndex; i--) {
			destinationArray[i] = (byte) (value & 0x000000ff);
			value = value >>> Byte.SIZE;
		}
	}

	public static byte[] short2bytes(short value) {
		byte[] result = new byte[Short.BYTES];
		NumberConversion.short2bytes(value, result, 0);
		return result;
	}

	public static void short2bytes(short value, byte[] destinationArray, int firstIndex) {
		for (int i = (firstIndex + Short.BYTES) - 1; i >= firstIndex; i--) {
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
