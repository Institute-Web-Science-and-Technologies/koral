package playground;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class Playground {

	public static void main(String[] args) {
		// File workingDir = new File("/home/danijank/Downloads/testdata");
		// RDFFileIterator iterator = new RDFFileIterator(workingDir, false,
		// null);
		// HashCoverCreator coverCreator = new HashCoverCreator(null);
		// coverCreator.createGraphCover(iterator, workingDir, 4);
		long longValue = 0x00_00_00_00_00_00_00_01l;
		short shortValue = Short.MIN_VALUE;
		byte[] longB = ByteBuffer.allocate(8).putLong(longValue).array();
		byte[] shortB = ByteBuffer.allocate(2).putShort(shortValue).array();
		System.arraycopy(shortB, 0, longB, 0, shortB.length);

		long newLong = shortValue;
		newLong = newLong << 48;
		newLong |= longValue;

		System.out.println(Arrays.toString(longB));
		System.out.println(Arrays
				.toString(ByteBuffer.allocate(8).putLong(newLong).array()));

		// TODO http://www.mapdb.org/doc/index.html
	}

}
