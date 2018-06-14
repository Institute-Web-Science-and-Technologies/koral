package playground;

import java.util.Arrays;
import java.util.function.LongBinaryOperator;

import de.uni_koblenz.west.koral.common.utils.RandomAccessRLEList;
import de.uni_koblenz.west.koral.common.utils.ReusableIDGenerator;

public class ReusableIDGeneratorTest {

	public static void main(String[] args) {
//		test1();
		test2();
	}

	static void test1() {
		long[] rle = new long[] { 3, -1, 1, -2, 5, -1, 2, -1, 10, 0 };
		long maxId = Arrays.stream(rle).reduce(0, new LongBinaryOperator() {
			@Override
			public long applyAsLong(long left, long right) {
				return left + Math.abs(right);
			}
		});
		ReusableIDGenerator rig = new ReusableIDGenerator(rle);
		rig.release(25);
		System.out.println(rig);
		System.out.println(ReusableIDGeneratorTest.visualizeRLE(rle));
		for (long id = 0; id <= (maxId + 5); id++) {
//			if (rig.isUsed(id)) {
			System.out.println(id + ":" + rig.usedIdsBefore(id));
//			}
		}
	}

	static void test2() {
		RandomAccessRLEList rlel = new RandomAccessRLEList();
		System.out.println(rlel);
		System.out.println("Set 0");
		rlel.set(0);
		System.out.println(rlel);
		System.out.println("Set 2");
		rlel.set(2);
		System.out.println(rlel);
		System.out.println("Set 3");
		rlel.set(3);
		System.out.println(rlel);

		System.out.println("New RLE");
		rlel = new RandomAccessRLEList();
		System.out.println("Set 4");
		rlel.set(4);
		System.out.println(rlel);
		System.out.println("Set 7");
		rlel.set(7);
		System.out.println(rlel);

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
