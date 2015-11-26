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

		long[] vars1 = new long[] { 1 };
		Mapping mapping1 = createMapping(cache, vars1, new long[] { 1 },
				new byte[] { 1, 0 });
		System.out.println(mapping1.toString(vars1));

		long[] vars2 = new long[] {};
		Mapping mapping2 = createMapping(cache, vars2, new long[] {},
				new byte[] { 1, 0 });
		System.out.println(mapping2.toString(vars2));

		long[] vars3 = new long[] {};
		Mapping mapping3 = cache.mergeMappings(vars3, mapping1, vars1, mapping2,
				vars2);
		System.out.println(mapping3.toString(vars3));
	}

	private static Mapping createMapping(MappingRecycleCache cache, long[] vars,
			long[] values, byte[] containment) {
		byte[] newMapping = new byte[Byte.BYTES + Long.BYTES + Long.BYTES
				+ Integer.BYTES + vars.length * Long.BYTES
				+ containment.length];
		newMapping[0] = MessageType.QUERY_MAPPING_BATCH.getValue();
		NumberConversion.int2bytes(newMapping.length, newMapping,
				Byte.BYTES + Long.BYTES + Long.BYTES);
		for (int i = 0; i < values.length; i++) {
			NumberConversion.long2bytes(values[i], newMapping, Byte.BYTES
					+ Long.BYTES + Long.BYTES + Integer.BYTES + i * Long.BYTES);
		}
		System.arraycopy(containment, 0, newMapping,
				newMapping.length - containment.length, containment.length);
		return cache.createMapping(newMapping, 0, newMapping.length);
	}

}
