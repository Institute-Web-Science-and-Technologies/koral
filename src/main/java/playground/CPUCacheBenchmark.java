package playground;

/**
 * This class tests the hypothesis that frequent copies of frequently used arrays might result in CPU cache misses
 * because the array moves into RAM after the CPU cache runs out of space.
 *
 * @author Philipp Töws
 *
 */
public class CPUCacheBenchmark {

	private static final int ARRAY_SIZE = 10_000;

	private static final int CLONES = 10_000_000;

	private static void read(byte[] array, int count) {
		int sum = 0;
		for (int n = 0; n < count; n++) {
			long start = System.nanoTime();
			for (int i = 0; i < array.length; i++) {
				sum += array[i];
			}
			if (count < 5) {
				long time = System.nanoTime() - start;
				System.out.println("Reading took " + String.format("%,d", time) + "ns");
			}
		}
		System.out.println(sum);
	}

	public static void main(String[] args) {
		System.out.println("Parameters:");
		System.out.println("Array size: " + String.format("%,d", ARRAY_SIZE));
		System.out.println("Clones: " + String.format("%,d", CLONES));
		byte[] array = new byte[ARRAY_SIZE];
		// Fill with some non-zero content
		for (int i = 0; i < array.length; i++) {
			array[i] = (byte) i;
		}
		// Make this array important, so it lands in CPU Cache
		read(array, 1_000_000);

		System.out.println("Initial Reads:");
		read(array, 3);

		// Dont put this into a method because the array variable reference would still point to the old array
		long start = System.nanoTime();
		for (int i = 0; i < CLONES; i++) {
			byte[] copy = new byte[array.length];
			System.arraycopy(array, 0, copy, 0, array.length);
			array = copy;
			if ((i % 100_000) == 0) {
				long time = System.nanoTime() - start;
				System.out.println("Cloning until #" + String.format("%,d", i) + ": " + String.format("%,d", time));
				start = System.nanoTime();
			}
			if (i == 0) {
				System.out.println("Reads after first cloning:");
				read(array, 3);
			}
		}

		System.out.println("Reads after lots of cloning:");
		read(array, 3);
	}

}
