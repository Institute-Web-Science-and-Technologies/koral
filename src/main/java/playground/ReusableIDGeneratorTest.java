package playground;

import java.util.Arrays;
import java.util.function.LongBinaryOperator;

import de.uni_koblenz.west.koral.common.utils.ReusableIDGenerator;

public class ReusableIDGeneratorTest {

	public static void main(String[] args) {
		test1();
		test2();
//		test3();
	}

	static void test1() {
		ReusableIDGenerator rig = new ReusableIDGenerator(null, 4, 10);
		set(rig, 0);
		release(rig, 0);
		set(rig, 1);
		set(rig, 2);
		set(rig, 2);
		set(rig, 4);
		set(rig, 4);
		release(rig, 4);
		set(rig, 5);
		release(rig, 2);
		release(rig, 1);
		set(rig, 0);
		set(rig, 8);
		release(rig, 4);
		set(rig, 2);
		set(rig, 1);
		set(rig, 3);
		set(rig, 7);
		release(rig, 0);
		set(rig, 9);
		release(rig, 2);
		release(rig, 8);
		set(rig, 0);
		set(rig, 10);
		set(rig, 12);
	}

	static void test2() {
		long[] rle = new long[] { 3, -1, 1, -2, 5, -1, 2, -1, 10, 0 };
		long maxId = Arrays.stream(rle).reduce(0, new LongBinaryOperator() {
			@Override
			public long applyAsLong(long left, long right) {
				return left + Math.abs(right);
			}
		});
		ReusableIDGenerator rig = new ReusableIDGenerator(rle, 4, 10);
		release(rig, 25);
		System.out.println(rig);
		System.out.println(ReusableIDGeneratorTest.visualizeRLE(rle));
		for (long id = 0; id <= (maxId + 5); id++) {
			if (rig.isUsed(id)) {
				System.out.println(id + ":" + rig.usedIdsBefore(id));
			}
		}
	}

	private static void set(ReusableIDGenerator rig, long id) {
		System.out.print("Set " + id);
		rig.set(id);
		System.out.println("\t" + rig);
	}

	private static void release(ReusableIDGenerator rig, long id) {
		System.out.print("Rel " + id);
		rig.release(id);
		System.out.println("\t" + rig);
	}

	static String visualizeRLE(long[] rle) {
		StringBuilder sb = new StringBuilder("[");
		for (int i = 0; i < rle.length; i++) {
			if (rle[i] == 0) {
				break;
			}
			char symbol = rle[i] > 0 ? 'X' : '-';
			for (int j = 0; j < Math.abs(rle[i]); j++) {
				sb.append(symbol);
			}
			if (i < (rle.length - 1)) {
				sb.append("|");
			}
		}
		sb.append("]");
		return sb.toString();
	}

}
