package playground;

import de.uni_koblenz.west.koral.common.utils.ReusableIDGenerator;

public class ReusableIDGeneratorTest {

	public static void main(String[] args) {
		ReusableIDGenerator rig = new ReusableIDGenerator(new long[] { 3, -1, 1, -1, 5, -1, 2, -1, 10, 0 });
		rig.release(21);
		System.out.println(rig);
	}

}
