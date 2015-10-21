package playground;

import java.io.File;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

public class Playground {

	public static void main(String[] args) {
		// File workingDir = new File("/home/danijank/Downloads/testdata");
		// RDFFileIterator iterator = new RDFFileIterator(workingDir, false,
		// null);
		// HashCoverCreator coverCreator = new HashCoverCreator(null);
		// coverCreator.createGraphCover(iterator, workingDir, 4);
		DB db = DBMaker.newFileDB(new File("/home/danijank/Downloads/test.db"))
				.transactionDisable().closeOnJvmShutdown()
				.mmapFileEnableIfSupported().asyncWriteEnable().make();
		try {
			HTreeMap<String, Long> map = db.createHashMap("encoder")
					.keySerializer(new Serializer.CompressionWrapper<>(
							Serializer.STRING))
					.valueSerializer(Serializer.LONG).makeOrGet();
			System.out.println(map.get("test"));
		} finally {
			db.close();
		}

		// TODO http://www.mapdb.org/doc/index.html
	}

}
