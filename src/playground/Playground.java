package playground;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.jena.query.QueryFactory;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.query.execution.QueryOperatorBase;
import de.uni_koblenz.west.cidre.common.query.execution.QueryOperatorTask;
import de.uni_koblenz.west.cidre.common.query.parser.QueryExecutionTreeType;
import de.uni_koblenz.west.cidre.common.query.parser.SparqlParser;
import de.uni_koblenz.west.cidre.common.query.parser.VariableDictionary;
import de.uni_koblenz.west.cidre.common.utils.RDFFileIterator;
import de.uni_koblenz.west.cidre.master.dictionary.DictionaryEncoder;
import de.uni_koblenz.west.cidre.master.graph_cover_creator.impl.HashCoverCreator;
import de.uni_koblenz.west.cidre.master.statisticsDB.GraphStatistics;
import de.uni_koblenz.west.cidre.slave.triple_store.TripleStoreAccessor;

/**
 * A class to test source code. Not used within CIDRE.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class Playground {

	public static void main(String[] args) {
		File workingDir = new File(System.getProperty("java.io.tmpdir")
				+ File.separator + "cidreTest");
		if (!workingDir.exists()) {
			workingDir.mkdir();
		}

		File inputFile = new File(args[0]);
		Configuration conf = new Configuration();
		conf.setDictionaryDir(
				workingDir.getAbsolutePath() + File.separator + "dictionary");
		conf.setStatisticsDir(
				workingDir.getAbsolutePath() + File.separator + "statistics");
		conf.setTripleStoreDir(
				workingDir.getAbsolutePath() + File.separator + "tripleStore");

		// create cover
		RDFFileIterator iterator = new RDFFileIterator(inputFile, false, null);
		HashCoverCreator coverCreator = new HashCoverCreator(null);
		File[] cover = coverCreator.createGraphCover(iterator, workingDir, 4);

		// encode cover and collect statistics
		DictionaryEncoder encoder = new DictionaryEncoder(conf, null);
		GraphStatistics statistics = new GraphStatistics(conf, (short) 4, null);
		File[] encodedFiles = encoder.encodeGraphChunks(cover, statistics,
				workingDir);

		// store triples
		TripleStoreAccessor accessor = new TripleStoreAccessor(conf, null);
		for (File file : encodedFiles) {
			if (file != null) {
				accessor.storeTriples(file);
			}
		}

		// process query
		String query = readQueryFromFile(new File(args[1]));
		query = QueryFactory.create(query).serialize();
		System.out.println(query);

		VariableDictionary dictionary = new VariableDictionary();
		SparqlParser parser = new SparqlParser(encoder, accessor, (short) 0, 0,
				0, 4, conf.getReceiverQueueSize(), workingDir,
				conf.getMaxEmittedMappingsPerRound(),
				conf.getNumberOfHashBuckets(),
				conf.getMaxInMemoryMappingsDuringJoin());
		QueryOperatorTask task = parser.parse(query,
				QueryExecutionTreeType.LEFT_LINEAR, dictionary);
		System.out.println(task.toString());

		System.out.println("==statistics==");
		System.out.println(
				"resource, subject0, ..., subject3, property0, ..., property3, object0, ..., object3");
		for (int i = 0; i < 20; i++) {
			for (int j = 0; j < 4; j++) {
				long r = ((long) j) << 48;
				r |= (i & 0x00_00_ff_ff_ff_ff_ff_ffl);
				if (encoder.decode(r) == null) {
					continue;
				}
				String rs = r + "";
				while (rs.length() < 15) {
					rs = " " + rs;
				}
				System.out.print(rs + "\t");
				System.out.print("[" + statistics.getSubjectFrequency(r, 0)
						+ "," + statistics.getSubjectFrequency(r, 1) + ","
						+ statistics.getSubjectFrequency(r, 2) + ","
						+ statistics.getSubjectFrequency(r, 3) + "]\t");
				System.out.print("[" + statistics.getPropertyFrequency(r, 0)
						+ "," + statistics.getPropertyFrequency(r, 1) + ","
						+ statistics.getPropertyFrequency(r, 2) + ","
						+ statistics.getPropertyFrequency(r, 3) + "]\t");
				System.out.println("[" + statistics.getObjectFrequency(r, 0)
						+ "," + statistics.getObjectFrequency(r, 1) + ","
						+ statistics.getObjectFrequency(r, 2) + ","
						+ statistics.getObjectFrequency(r, 3) + "]");
			}
		}
		System.out.println("ownerload:[" + statistics.getOwnerLoad(0) + ","
				+ statistics.getOwnerLoad(1) + "," + statistics.getOwnerLoad(2)
				+ "," + statistics.getOwnerLoad(3) + "]");

		for (int i = 0; i < 4; i++) {
			System.out.println("Slave " + i + ":");
			((QueryOperatorBase) task).adjustEstimatedLoad(statistics, i);
			System.out.println(task.toString());
		}

		encoder.close();
		statistics.close();
		accessor.close();

		delete(workingDir);
	}

	private static void delete(File dir) {
		for (File file : dir.listFiles()) {
			if (file.isDirectory()) {
				delete(file);
			} else {
				file.delete();
			}
		}
		dir.delete();
	}

	private static String readQueryFromFile(File queryFile) {
		try (BufferedReader br = new BufferedReader(
				new FileReader(queryFile));) {
			StringBuilder sb = new StringBuilder();
			String delim = "";
			for (String line = br.readLine(); line != null; line = br
					.readLine()) {
				sb.append(delim);
				sb.append(line);
				delim = "\n";
			}
			return sb.toString();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
