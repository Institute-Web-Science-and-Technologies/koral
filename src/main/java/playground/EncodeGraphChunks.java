package playground;

import java.io.File;

import de.uni_koblenz.west.koral.common.config.impl.Configuration;
import de.uni_koblenz.west.koral.common.utils.GraphFileFilter;
import de.uni_koblenz.west.koral.master.dictionary.DictionaryEncoder;
import de.uni_koblenz.west.koral.master.graph_cover_creator.GraphCoverCreator;
import de.uni_koblenz.west.koral.master.graph_cover_creator.impl.HashCoverCreator;

public class EncodeGraphChunks {

	public static void main(String[] args) {
		if (args.length != 3) {
			System.out.println("Usage: java " + EncodeGraphChunks.class.getSimpleName()
					+ " <inputFile> <outputDir> <numberOfChunks>");
			return;
		}
		File inputFile = new File(args[0]);
		String outputDirPath = args[1].endsWith(File.separator) ? args[1] : args[1] + File.separator;
		File outputDir = new File(outputDirPath + inputFile.getName().split("\\.")[0]);
		outputDir.mkdirs();

		short numberOfChunks = Short.parseShort(args[2]);

		Configuration conf = new Configuration();

		GraphCoverCreator coverCreator = new HashCoverCreator(null, null);

		try (DictionaryEncoder encoder = new DictionaryEncoder(conf, null, null);) {
			System.out.println("Encoding...");
			File encodedInput = encoder.encodeOriginalGraphFiles(
					inputFile.isDirectory() ? inputFile.listFiles(new GraphFileFilter()) : new File[] { inputFile },
					outputDir, coverCreator.getRequiredInputEncoding(), numberOfChunks);

			System.out.println("Creating Graph Cover...");
			File[] graphCover = coverCreator.createGraphCover(encoder, encodedInput, outputDir, numberOfChunks);
			coverCreator.close();

			System.out.println("Encode graph chunks...");
			File[] encodedFiles = encoder.encodeGraphChunksCompletely(graphCover, outputDir,
					coverCreator.getRequiredInputEncoding());
			System.out.println("Encoded Files:");
			for (File f : encodedFiles) {
				System.out.println(f.getName());
			}
			System.out.println("Finished.");
		}

	}

}
