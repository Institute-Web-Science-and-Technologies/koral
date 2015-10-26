package de.uni_koblenz.west.cidre.master.dictionary;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PushbackInputStream;
import java.nio.ByteBuffer;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.jena.graph.Node;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.utils.RDFFileIterator;
import de.uni_koblenz.west.cidre.master.dictionary.impl.MapDBDictionary;
import de.uni_koblenz.west.cidre.master.statisticsDB.GraphStatistics;
import de.uni_koblenz.west.cidre.master.utils.DeSerializer;

/**
 * Triple Nodes are encoded using {@link DeSerializer}
 */
public class DictionaryEncoder implements Closeable {

	private final Logger logger;

	private final Dictionary dictionary;

	public DictionaryEncoder(Configuration conf, Logger logger) {
		this.logger = logger;
		dictionary = new MapDBDictionary(conf.getDictionaryStorageType(),
				conf.getDictionaryDataStructure(), conf.getDictionaryDir(),
				conf.useTransactionsForDictionary(),
				conf.isDictionaryAsynchronouslyWritten(),
				conf.getDictionaryCacheType());
	}

	public File[] encodeGraphChunks(File[] plainGraphChunks,
			GraphStatistics statistics) {
		File[] itermediateFiles = encodeGraphChunksAndCountStatistics(
				plainGraphChunks, statistics);
		return setOwnership(itermediateFiles, statistics);
	}

	private File[] encodeGraphChunksAndCountStatistics(File[] plainGraphChunks,
			GraphStatistics statistics) {
		File[] intermediateFiles = new File[plainGraphChunks.length];
		for (int i = 0; i < plainGraphChunks.length; i++) {
			if (plainGraphChunks[i] == null) {
				continue;
			}
			intermediateFiles[i] = new File(
					plainGraphChunks[i].getParentFile().getAbsolutePath()
							+ File.separatorChar + "chunk" + i + ".enc.int.gz");
			try (RDFFileIterator iter = new RDFFileIterator(plainGraphChunks[i],
					logger);
					OutputStream out = new BufferedOutputStream(
							new GZIPOutputStream(new FileOutputStream(
									intermediateFiles[i])));) {
				for (Node[] quad : iter) {
					long subject = dictionary
							.encode(DeSerializer.serializeNode(quad[0]));
					long property = dictionary
							.encode(DeSerializer.serializeNode(quad[1]));
					long object = dictionary
							.encode(DeSerializer.serializeNode(quad[2]));
					statistics.count(subject, property, object, i);
					byte[] containment = DeSerializer
							.deserializeBitSetFromNode(quad[3]).toByteArray();
					out.write(ByteBuffer.allocate(8).putLong(subject).array());
					out.write(ByteBuffer.allocate(8).putLong(property).array());
					out.write(ByteBuffer.allocate(8).putLong(object).array());
					out.write(ByteBuffer.allocate(2)
							.putShort((short) containment.length).array());
					out.write(containment);
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		return intermediateFiles;
	}

	private File[] setOwnership(File[] intermediateFiles,
			GraphStatistics statistics) {
		File[] encodedFiles = new File[intermediateFiles.length];
		for (int i = 0; i < intermediateFiles.length; i++) {
			if (intermediateFiles[i] == null) {
				continue;
			}
			encodedFiles[i] = new File(
					intermediateFiles[i].getParentFile().getAbsolutePath()
							+ File.separatorChar + "chunk" + i + ".enc.gz");
			try (PushbackInputStream in = new PushbackInputStream(
					new BufferedInputStream(new GZIPInputStream(
							new FileInputStream(intermediateFiles[i]))),
					1);
					OutputStream out = new BufferedOutputStream(
							new GZIPOutputStream(
									new FileOutputStream(encodedFiles[i])));) {
				while (!isFinished(in)) {
					long subject = readLong(in);
					long property = readLong(in);
					long object = readLong(in);
					byte[] containment = readByteArray(in);

					short sOwner = statistics.getOwner(subject);
					short pOwner = statistics.getOwner(property);
					short oOwner = statistics.getOwner(object);

					long newSubject = dictionary.setOwner(subject, sOwner);
					statistics.setOwner(subject, sOwner);
					long newProperty = dictionary.setOwner(property, pOwner);
					statistics.setOwner(property, pOwner);
					long newObject = dictionary.setOwner(object, oOwner);
					statistics.setOwner(object, oOwner);

					out.write(
							ByteBuffer.allocate(8).putLong(newSubject).array());
					out.write(ByteBuffer.allocate(8).putLong(newProperty)
							.array());
					out.write(
							ByteBuffer.allocate(8).putLong(newObject).array());
					out.write(ByteBuffer.allocate(2)
							.putShort((short) containment.length).array());
					out.write(containment);
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			intermediateFiles[i].delete();
		}
		return encodedFiles;
	}

	private boolean isFinished(PushbackInputStream in) throws IOException {
		int nextByte = in.read();
		boolean isFinished = nextByte == -1;
		in.unread(nextByte);
		return isFinished;
	}

	private long readLong(InputStream in) throws IOException {
		byte[] longBytes = new byte[8];
		readBytes(in, longBytes);
		return ByteBuffer.wrap(longBytes).getLong();
	}

	private byte[] readByteArray(InputStream in) throws IOException {
		byte[] lengthBytes = new byte[2];
		readBytes(in, lengthBytes);
		int length = ByteBuffer.wrap(lengthBytes).getShort();
		byte[] byteArray = new byte[length];
		readBytes(in, byteArray);
		return byteArray;
	}

	private void readBytes(InputStream in, byte[] bytesToRead)
			throws IOException {
		int readBytes = 0;
		int totalBytesRead = 0;
		do {
			readBytes = in.read(bytesToRead, totalBytesRead,
					bytesToRead.length - totalBytesRead);
			if (readBytes > 0) {
				totalBytesRead += readBytes;
			}
		} while (readBytes != -1 && totalBytesRead < bytesToRead.length);
		if (readBytes == -1) {
			throw new IOException(
					"InputStream has ended before value could be read completely.");
		}
	}

	public Node decode(long id) {
		String plainText = dictionary.decode(id);
		if (plainText == null) {
			return null;
		}
		return DeSerializer.deserializeNode(plainText);
	}

	public void clear() {
		dictionary.clear();
	}

	@Override
	public void close() {
		dictionary.close();
	}

}
