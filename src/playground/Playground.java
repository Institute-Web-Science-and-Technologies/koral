package playground;

import de.uni_koblenz.west.cidre.master.dictionary.Dictionary;
import de.uni_koblenz.west.cidre.master.dictionary.impl.MapDBCacheOptions;
import de.uni_koblenz.west.cidre.master.dictionary.impl.MapDBDataStructureOptions;
import de.uni_koblenz.west.cidre.master.dictionary.impl.MapDBDictionary;
import de.uni_koblenz.west.cidre.master.dictionary.impl.MapDBStorageOptions;

public class Playground {

	public static void main(String[] args) {
		// File workingDir = new File("/home/danijank/Downloads/testdata");
		// RDFFileIterator iterator = new RDFFileIterator(workingDir, false,
		// null);
		// HashCoverCreator coverCreator = new HashCoverCreator(null);
		// coverCreator.createGraphCover(iterator, workingDir, 4);

		Dictionary dictionary = new MapDBDictionary(
				MapDBStorageOptions.MEMORY_MAPPED_FILE,
				MapDBDataStructureOptions.HASH_TREE_MAP,
				"/home/danijank/Downloads/dictionary", false, true,
				MapDBCacheOptions.HASH_TABLE);

		String value = "Test";
		// long oldID = dictionary.encode(value);
		// System.out.println("initial ID is " + oldID + " = " + Arrays
		// .toString(ByteBuffer.allocate(8).putLong(oldID).array()));
		// System.out.println(
		// "encoding(" + value + ") = " + dictionary.encode(value));
		// System.out.println(
		// "decoding(" + oldID + ") = " + dictionary.decode(oldID));
		// short owner = 1;
		// long newID = dictionary.setOwner(oldID, owner);
		// System.out.println("new ID is " + newID + " = " + Arrays
		// .toString(ByteBuffer.allocate(8).putLong(newID).array()));
		System.out.println(
				"encoding(" + value + ") = " + dictionary.encode(value));
		// System.out.println(
		// "decoding(" + oldID + ") = " + dictionary.decode(oldID));
		// System.out.println(
		// "decoding(" + newID + ") = " + dictionary.decode(newID));
		System.out.println("decoding(" + 0 + ") = " + dictionary.decode(0));
		System.out.println("decoding(" + 281474976710656l + ") = "
				+ dictionary.decode(281474976710656l));

		// TODO http://www.mapdb.org/doc/index.html
	}

}
