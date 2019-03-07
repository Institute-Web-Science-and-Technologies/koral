package playground;

public class PolymorphicNumbersBenchmark {

	public PolymorphicNumbersBenchmark() {}

	public static void main(String[] args) {
		long antiOptimizer = 0;
		long timeInt = 0;
		long timeNumber = 0;
		long timeNumberCast = 0;
		long timeLong = 0;
		long start = 0;

		for (long j = 0; j < 100_000_000; j++) {
			int i = (int) j;
			start = System.nanoTime();
			antiOptimizer += funcNumber(i);
			timeNumber += System.nanoTime() - start;
		}
		for (long j = 0; j < 100_000_000; j++) {
			int i = (int) j;

			start = System.nanoTime();
			antiOptimizer += funcNumberCast(i);
			timeNumberCast += System.nanoTime() - start;
		}

		for (long j = 0; j < 100_000_000; j++) {
			byte i = (byte) j;
			start = System.nanoTime();
			antiOptimizer += funcLong(i);
			timeLong += System.nanoTime() - start;
		}

		for (long j = 0; j < 100_000_000; j++) {
			byte i = (byte) j;
			start = System.nanoTime();
			antiOptimizer += funcInt(i);
			timeInt += System.nanoTime() - start;
		}

		System.out.println("Int: " + String.format("%,d", timeInt));
		System.out.println("Number: " + String.format("%,d", timeNumber));
		System.out.println("Number Cast: " + String.format("%,d", timeNumberCast));
		System.out.println("Long: " + String.format("%,d", timeLong));
		System.out.println(antiOptimizer);
	}

	private static byte funcInt(byte i) {
		return i;
	}

	private static int funcNumber(Number i) {
		return i.intValue();
	}

	private static int funcNumberCast(Number i) {
		return (int) i;
	}

	private static byte funcLong(long i) {
		return (byte) i;
	}

}
