package playground;

import de.uni_koblenz.west.cidre.common.utils.ReusableIDGenerator;

/**
 * A class to test source code. Not used within CIDRE.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class Playground {

	public static void main(String[] args) {
		ReusableIDGenerator ids = new ReusableIDGenerator();
		System.out.println(ids);
		for (int i = 0; i < 100; i++) {
			allocate(ids);
		}

		release(ids, 50);
		release(ids, 51);

		release(ids, 70);
		release(ids, 71);

		allocate(ids);
		allocate(ids);
		allocate(ids);
		allocate(ids);
	}

	private static void allocate(ReusableIDGenerator ids) {
		int id = ids.getNextId();
		System.out.println(id + "<" + ids);
	}

	private static void release(ReusableIDGenerator ids, int id) {
		ids.release(id);
		System.out.println(id + ">" + ids);
	}

}
