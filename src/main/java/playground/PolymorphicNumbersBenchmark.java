package playground;

public class PolymorphicNumbersBenchmark {

  public PolymorphicNumbersBenchmark() {
  }

  public static void main(String[] args) {
    // parameterTypeBenchmark();
    PolymorphicNumbersBenchmark.returnTypeBenchmark();
  }

  @SuppressWarnings("unused")
  private static void parameterTypeBenchmark() {
    long antiOptimizer = 0;
    long timeInt = 0;
    long timeNumber = 0;
    long timeNumberCast = 0;
    long timeLong = 0;
    long start = 0;

    for (long j = 0; j < 100_000_000; j++) {
      int i = (int) j;
      start = System.nanoTime();
      antiOptimizer += PolymorphicNumbersBenchmark.funcNumber(i);
      timeNumber += System.nanoTime() - start;
    }
    for (long j = 0; j < 100_000_000; j++) {
      int i = (int) j;

      start = System.nanoTime();
      antiOptimizer += PolymorphicNumbersBenchmark.funcNumberCast(i);
      timeNumberCast += System.nanoTime() - start;
    }

    for (long j = 0; j < 100_000_000; j++) {
      byte i = (byte) j;
      start = System.nanoTime();
      antiOptimizer += PolymorphicNumbersBenchmark.funcLong(i);
      timeLong += System.nanoTime() - start;
    }

    for (long j = 0; j < 100_000_000; j++) {
      byte i = (byte) j;
      start = System.nanoTime();
      antiOptimizer += PolymorphicNumbersBenchmark.funcInt(i);
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

  private static void returnTypeBenchmark() {
    byte byteAntiOptimizer = 0;
    int intAntiOptimizer = 0;
    long longAntiOptimizer = 0;
    long timeByte = 0;
    long timeInt = 0;
    long timeLong = 0;
    long start = 0;

    for (long j = 0; j < 100_000_000; j++) {
      byte i = (byte) j;
      start = System.nanoTime();
      longAntiOptimizer += PolymorphicNumbersBenchmark.upcastLong(i);
      timeLong += System.nanoTime() - start;
    }
    System.out.println("upcast long: " + String.format("%,d", timeLong));

    for (long j = 0; j < 100_000_000; j++) {
      byte i = (byte) j;
      start = System.nanoTime();
      intAntiOptimizer += PolymorphicNumbersBenchmark.upcastInt(i);
      timeInt += System.nanoTime() - start;
    }
    System.out.println("upcast int: " + String.format("%,d", timeInt));

    for (long j = 0; j < 100_000_000; j++) {
      byte i = (byte) j;
      start = System.nanoTime();
      byteAntiOptimizer += PolymorphicNumbersBenchmark.noUpcast(i);
      timeByte += System.nanoTime() - start;
    }
    System.out.println("no upcast: " + String.format("%,d", timeByte));

    System.out.println(byteAntiOptimizer);
    System.out.println(intAntiOptimizer);
    System.out.println(longAntiOptimizer);
  }

  private static byte noUpcast(byte b) {
    return b;
  }

  private static long upcastLong(byte b) {
    return b;
  }

  private static int upcastInt(byte b) {
    return b;
  }

}
