package playground;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;

import de.uni_koblenz.west.koral.common.config.impl.Configuration;
import de.uni_koblenz.west.koral.common.utils.GraphFileFilter;
import de.uni_koblenz.west.koral.master.dictionary.DictionaryEncoder;
import de.uni_koblenz.west.koral.master.graph_cover_creator.GraphCoverCreator;
import de.uni_koblenz.west.koral.master.graph_cover_creator.impl.HashCoverCreator;

public class EncodeGraphChunks {

	public EncodeGraphChunks() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		if (args.length == 0) {
			System.out.println("Missing input file");
			return;
		}
		if (args.length == 1) {
			System.out.println("Missing output directory");
			return;
		}
		File inputFile = new File(args[0]);
		String workingDirPath = args[1].endsWith(File.separator) ? args[1] : args[1] + File.separator;
		File workingDir = new File(workingDirPath + inputFile.getName().split(".")[0]);
		File masterDir = new File("/tmp/master");
		File slaveDir = new File("/tmp/slave");
		try {
			if (masterDir.exists()) {
//				FileUtils.cleanDirectory(masterDir);
			}
			if (slaveDir.exists()) {
				FileUtils.cleanDirectory(slaveDir);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		workingDir.mkdir();

		short numberOfChunks = 4;

		Configuration conf = new Configuration();

		GraphCoverCreator coverCreator = new HashCoverCreator(null, null);

		try (DictionaryEncoder encoder = new DictionaryEncoder(conf, null, null);) {
			System.out.println("Encoding...");
			File encodedInput = encoder.encodeOriginalGraphFiles(
					inputFile.isDirectory() ? inputFile.listFiles(new GraphFileFilter()) : new File[] { inputFile },
					workingDir, coverCreator.getRequiredInputEncoding(), numberOfChunks);

			System.out.println("Creating Graph Cover...");
			File[] graphCover = coverCreator.createGraphCover(encoder, encodedInput, workingDir, numberOfChunks);
			coverCreator.close();

			System.out.println("Encode graph chunks...");
			File[] encodedFiles = encoder.encodeGraphChunksCompletely(graphCover, workingDir,
					coverCreator.getRequiredInputEncoding());
			System.out.println("Encoded Files:\n" + Arrays.toString(encodedFiles));
		}

	}

}
