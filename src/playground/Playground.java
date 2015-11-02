package playground;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;

import org.apache.jena.graph.Node;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.query.TriplePattern;
import de.uni_koblenz.west.cidre.common.utils.RDFFileIterator;
import de.uni_koblenz.west.cidre.master.dictionary.DictionaryEncoder;
import de.uni_koblenz.west.cidre.master.graph_cover_creator.impl.HashCoverCreator;
import de.uni_koblenz.west.cidre.master.statisticsDB.GraphStatistics;
import de.uni_koblenz.west.cidre.master.utils.DeSerializer;
import de.uni_koblenz.west.cidre.slave.triple_store.TripleStoreAccessor;
import de.uni_koblenz.west.cidre.slave.triple_store.impl.MappingIteratorWrapper;

public class Playground {

	public static void main(String[] args) {
		File workingDir = new File("/home/danijank/Downloads/testdata");
		RDFFileIterator iterator = new RDFFileIterator(workingDir, false, null);
		HashCoverCreator coverCreator = new HashCoverCreator(null);
		File[] cover = coverCreator.createGraphCover(iterator, workingDir, 4);
		Configuration conf = new Configuration();
		conf.setDictionaryDir("/home/danijank/Downloads/testdata/dictionary");
		conf.setStatisticsDir("/home/danijank/Downloads/testdata/statistics");
		conf.setTripleStoreDir("/home/danijank/Downloads/testdata/tripleStore");
		DictionaryEncoder encoder = new DictionaryEncoder(conf, null);
		GraphStatistics statistics = new GraphStatistics(conf, (short) 4, null);
		File[] encodedFiles = encoder.encodeGraphChunks(cover, statistics,
				new File("/home/danijank/Downloads/testdata/"));
		TripleStoreAccessor accessor = new TripleStoreAccessor(conf, null);
		for (File file : encodedFiles) {
			if (file != null) {
				accessor.storeTriples(file);
			}
		}

		MappingIteratorWrapper wrapper = (MappingIteratorWrapper) accessor
				.lookup(null, new TriplePattern()).iterator();

		try {
			Field iterField = wrapper.getClass().getDeclaredField("iter");
			iterField.setAccessible(true);
			@SuppressWarnings("unchecked")
			Iterator<byte[]> iter = (Iterator<byte[]>) iterField.get(wrapper);

			while (iter.hasNext()) {
				byte[] next = iter.next();
				ByteBuffer buffer = ByteBuffer.wrap(next);
				System.out.println(encoder.decode(buffer.getLong()) + " "
						+ encoder.decode(buffer.getLong()) + " "
						+ encoder.decode(buffer.getLong()) + " "
						+ BitSet.valueOf(
								Arrays.copyOfRange(next, 3 * 8, next.length)));
			}

		} catch (NoSuchFieldException | SecurityException
				| IllegalArgumentException | IllegalAccessException e) {
			e.printStackTrace();
		}

		encoder.close();
		statistics.close();
		accessor.close();
	}

	public static void decodeFile(DictionaryEncoder dictionary,
			File[] encodedFiles) {
		for (File file : encodedFiles) {
			try (DataInputStream input = new DataInputStream(
					new BufferedInputStream(
							new GZIPInputStream(new FileInputStream(file))));
					BufferedWriter output = new BufferedWriter(new FileWriter(
							new File(file.getAbsolutePath() + ".nq")));) {
				while (true) {
					try {
						Node subject = dictionary.decode(input.readLong());
						Node property = dictionary.decode(input.readLong());
						Node object = dictionary.decode(input.readLong());

						short length = input.readShort();
						byte[] containment = new byte[length];
						input.readFully(containment);
						BitSet cntment = BitSet.valueOf(containment);
						Node cntmentNode = DeSerializer
								.serializeBitSetAsNode(cntment);

						output.write(DeSerializer.serializeNode(subject));
						output.write(" ");
						output.write(DeSerializer.serializeNode(property));
						output.write(" ");
						output.write(DeSerializer.serializeNode(object));
						output.write(" ");
						output.write(DeSerializer.serializeNode(cntmentNode));
						output.write(" .\n");
					} catch (EOFException e) {
						break;
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
