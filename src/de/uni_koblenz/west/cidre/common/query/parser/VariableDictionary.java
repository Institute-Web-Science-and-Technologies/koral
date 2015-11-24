package de.uni_koblenz.west.cidre.common.query.parser;

/**
 * Dictionary to decode and encode variable names.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class VariableDictionary {

	private int nextID;

	private String[] id2name;

	public VariableDictionary() {
		id2name = new String[10];
		nextID = 0;
	}

	public long encode(String varName) {
		if ((nextID & 0xff_ff_00_00) != 0) {
			throw new ArrayIndexOutOfBoundsException(
					"The maximum amount of variables has already been encoded.");
		}
		int id = -1;
		// check existence of id
		for (int i = 0; i <= nextID; i++) {
			if (id2name[i].equals(varName)) {
				id = i;
				break;
			}
		}
		if (id == -1) {
			// create new id
			id = nextID++;
			if (id >= id2name.length) {
				String[] newId2name = new String[id2name.length + 10];
				System.arraycopy(id2name, 0, newId2name, 0, id2name.length);
				id2name = newId2name;
			}
			id2name[id] = varName;
		}
		return id & 0x00_00_00_00_ff_ff_ff_ffl;
	}

	public String decode(long var) {
		int index = (int) var;
		if (index >= nextID) {
			throw new IllegalArgumentException(
					"The variable " + var + " is unknown.");
		}
		return id2name[index];
	}

	public String[] decode(long[] vars) {
		String[] result = new String[vars.length];
		for (int i = 0; i < vars.length; i++) {
			result[i] = decode(vars[i]);
		}
		return result;
	}

}
