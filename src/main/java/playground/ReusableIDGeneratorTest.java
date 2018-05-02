package playground;

import java.util.Arrays;
import java.util.function.LongBinaryOperator;

import de.uni_koblenz.west.koral.common.utils.ReusableIDGenerator;

public class ReusableIDGeneratorTest {

	public static void main(String[] args) {
		long[] rle = new long[] { 3, -1, 1, -1, 5, -1, 2, -1, 10, 0 };
		long maxId = Arrays.stream(rle).reduce(0, new LongBinaryOperator() {

			@Override
			public long applyAsLong(long left, long right) {
				return left + Math.abs(right);
			}
		});
		ReusableIDGenerator rig = new ReusableIDGenerator(rle);
		System.out.println(rig);
		for (long id = -3; id <= (maxId + 5); id++) {
			System.out.println(id + ":" + rig.isUsed(id));
		}
	}

}
