package playground;

import de.uni_koblenz.west.cidre.common.messages.MessageType;
import de.uni_koblenz.west.cidre.common.query.Mapping;
import de.uni_koblenz.west.cidre.common.query.MappingRecycleCache;
import de.uni_koblenz.west.cidre.common.utils.NumberConversion;

/**
 * A class to test source code. Not used within CIDRE.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class Playground {

	public static void main(String[] args) {
		MappingRecycleCache cache = new MappingRecycleCache(10, 15);

		long[] vars1 = new long[] { 1, 2 };
		Mapping mapping1 = createMapping(cache, vars1, new long[] { 1, 2 },
				new byte[] { 1, 0 });
		System.out.println(mapping1.toString(vars1));
		System.out.println(mapping1.isKnownByComputer(7));
		System.out.println(mapping1.toString(vars1));
	}

	@SuppressWarnings("unused")
	private static long[] joinVars(long[] vars1, long[] vars2, long[] join) {
		long[] result = new long[vars1.length + vars2.length - join.length];
		System.arraycopy(vars1, 0, result, 0, vars1.length);
		int next = vars1.length;
		for (int i = 0; i < vars2.length; i++) {
			boolean isJoinVar = false;
			for (long var : join) {
				if (var == vars2[i]) {
					isJoinVar = true;
					break;
				}
			}
			if (isJoinVar) {
				continue;
			} else {
				result[next++] = vars2[i];
			}
		}
		return result;
	}

	private static Mapping createMapping(MappingRecycleCache cache, long[] vars,
			long[] values, byte[] containment) {
		byte[] newMapping = new byte[Byte.BYTES + Long.BYTES + Long.BYTES
				+ Integer.BYTES + vars.length * Long.BYTES + containment.length
				+ 2];
		newMapping[1] = MessageType.QUERY_MAPPING_BATCH.getValue();
		NumberConversion.int2bytes(newMapping.length, newMapping,
				1 + Byte.BYTES + Long.BYTES + Long.BYTES);
		for (int i = 0; i < values.length; i++) {
			NumberConversion.long2bytes(values[i], newMapping, 1 + Byte.BYTES
					+ Long.BYTES + Long.BYTES + Integer.BYTES + i * Long.BYTES);
		}
		System.arraycopy(containment, 0, newMapping,
				newMapping.length - containment.length - 1, containment.length);
		newMapping[0] = -1;
		newMapping[newMapping.length - 1] = -1;
		return cache.createMapping(newMapping, 1, newMapping.length - 2);
	}

}
