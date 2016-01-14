package playground;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;

import de.uni_koblenz.west.cidre.common.config.impl.Configuration;
import de.uni_koblenz.west.cidre.common.logger.CSVFileHandler;
import de.uni_koblenz.west.cidre.common.logger.LoggerFactory;
import de.uni_koblenz.west.cidre.common.utils.GraphFileFilter;
import de.uni_koblenz.west.cidre.common.utils.RDFFileIterator;

public class PreprocessBTC2014 {

	public static void main(String[] args) {
		if (args.length != 2) {
			System.out
					.println("Usage: java " + PreprocessBTC2014.class.getName()
							+ " <inputDir> <outputDir>");
			return;
		}
		File inputDir = new File(args[0]);
		if (!inputDir.exists()) {
			inputDir.mkdirs();
		}
		if (inputDir.listFiles().length == 0) {
			downloadDataset(inputDir);
		}

		File outputDir = new File(args[1]);
		if (!outputDir.exists()) {
			outputDir.mkdirs();
		}
		preprocessFiles(inputDir, outputDir);
	}

	private static void preprocessFiles(File inputDir, File outputDir) {
		Configuration conf = new Configuration();
		conf.setLoglevel(Level.ALL);
		conf.setLogDirectory(outputDir.getAbsolutePath());
		DatasetGraph graph = DatasetGraphFactory.createMem();
		if (!inputDir.exists()) {
			throw new RuntimeException("The input directory "
					+ inputDir.getAbsolutePath() + " does not exist.");
		}
		if (inputDir.isFile()) {
			preprocessFile(inputDir, outputDir, conf, graph);
		} else {
			for (File inputFile : inputDir.listFiles(new GraphFileFilter())) {
				preprocessFile(inputFile, outputDir, conf, graph);
			}
		}
	}

	private static void preprocessFile(File inputFile, File outputDir,
			Configuration conf, DatasetGraph graph) {
		Logger csvFileLogger = null;
		try {
			csvFileLogger = LoggerFactory.getCSVFileLogger(conf,
					new String[] { "cleaned", inputFile.getName() },
					PreprocessBTC2014.class.getName());
			try (RDFFileIterator iterator = new RDFFileIterator(inputFile,
					false, csvFileLogger);
					BufferedOutputStream output = new BufferedOutputStream(
							new GZIPOutputStream(new FileOutputStream(
									outputDir.getAbsolutePath() + File.separator
											+ "cleaned_"
											+ inputFile.getName())));) {
				for (Node[] tuple : iterator) {
					if (tuple.length == 3) {
						Graph defaultGraph = graph.getDefaultGraph();
						defaultGraph
								.add(new Triple(tuple[0], tuple[1], tuple[2]));
					} else {
						graph.add(new Quad(tuple[3], tuple[0], tuple[1],
								tuple[2]));
					}
					RDFDataMgr.write(output, graph, RDFFormat.NQ);
					graph.clear();
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			if (csvFileLogger != null) {
				for (Handler handler : csvFileLogger.getHandlers()) {
					if (handler instanceof CSVFileHandler) {
						handler.flush();
						csvFileLogger.removeHandler(handler);
						handler.close();
					}
				}
			}
		}
	}

	private static void downloadDataset(File inputDir) {
		try (LineNumberReader urlInput = new LineNumberReader(
				new BufferedReader(new InputStreamReader(
						getInputStream(
								"http://km.aifb.kit.edu/projects/btc-2014/000-CONTENTS"),
						Charset.forName("UTF-8"))))) {
			for (String line = urlInput
					.readLine(); line != null; line = urlInput.readLine()) {
				if (!line.contains("/data.nq")) {
					continue;
				}
				downloadFile(getOutputFile(inputDir, line), line);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static void downloadFile(File outputFile, String urlOfFile)
			throws IOException {
		try (BufferedInputStream input = new BufferedInputStream(
				getInputStream(urlOfFile));
				BufferedOutputStream output = new BufferedOutputStream(
						new FileOutputStream(outputFile))) {
			byte[] buffer = new byte[4096];
			for (int readBytes = input.read(
					buffer); readBytes != -1; readBytes = input.read(buffer)) {
				output.write(buffer, 0, readBytes);
			}
		}
	}

	private static File getOutputFile(File inputDir, String urlOfFile) {
		String[] pathHierarchy = urlOfFile.split(Pattern.quote("/"));
		String[] filenameParts = pathHierarchy[pathHierarchy.length - 1]
				.split("\\.|\\-");
		StringBuilder outputFileName = new StringBuilder();
		outputFileName.append(inputDir.getAbsolutePath())
				.append(File.separator);
		outputFileName.append(pathHierarchy[pathHierarchy.length - 2])
				.append(filenameParts[0]);
		for (int i = 2; i < filenameParts.length - 1; i++) {
			outputFileName.append("-").append(filenameParts[i]);
		}
		outputFileName.append(".").append(filenameParts[1]).append(".")
				.append(filenameParts[filenameParts.length - 1]);
		return new File(outputFileName.toString());
	}

	private static InputStream getInputStream(String url) throws IOException {
		HttpURLConnection connection = (HttpURLConnection) new URL(url)
				.openConnection();
		connection.setInstanceFollowRedirects(true);
		int status = connection.getResponseCode();
		if (status != HttpURLConnection.HTTP_OK) {
			if (status == HttpURLConnection.HTTP_MOVED_TEMP
					|| status == HttpURLConnection.HTTP_MOVED_PERM
					|| status == HttpURLConnection.HTTP_SEE_OTHER) {
				// Process redirects
				return getInputStream(connection.getHeaderField("Location"));
			}
		}
		return connection.getInputStream();
	}

}
