package de.uni_koblenz.west.cidre.master.tasks;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

import de.uni_koblenz.west.cidre.common.messages.MessageType;
import de.uni_koblenz.west.cidre.common.messages.MessageUtils;
import de.uni_koblenz.west.cidre.common.utils.RDFFileIterator;
import de.uni_koblenz.west.cidre.master.client_manager.ClientConnectionManager;
import de.uni_koblenz.west.cidre.master.client_manager.FileReceiver;
import de.uni_koblenz.west.cidre.master.graph_cover_creator.CoverStrategyType;
import de.uni_koblenz.west.cidre.master.graph_cover_creator.GraphCoverCreator;
import de.uni_koblenz.west.cidre.master.graph_cover_creator.GraphCoverCreatorFactory;

public class GraphLoaderTask extends Thread implements Closeable {

	private final Logger logger;

	private final int clientId;

	private final ClientConnectionManager clientConnections;

	private final File workingDir;

	private CoverStrategyType coverStrategy;

	private int replicationPathLength;

	private int numberOfGraphChunks;

	private FileReceiver fileReceiver;

	private ClientConnectionKeepAliveTask keepAliveThread;

	private boolean graphIsLoadingOrLoaded;

	public GraphLoaderTask(int clientID,
			ClientConnectionManager clientConnections, File tmpDir,
			Logger logger) {
		isDaemon();
		graphIsLoadingOrLoaded = true;
		clientId = clientID;
		this.clientConnections = clientConnections;
		this.logger = logger;
		workingDir = new File(tmpDir.getAbsolutePath() + File.separatorChar
				+ "cidre_client_" + clientId);
		if (workingDir.exists()) {
			deleteContent(workingDir);
		} else {
			if (!workingDir.mkdirs()) {
				throw new RuntimeException(
						"The working directory " + workingDir.getAbsolutePath()
								+ " could not be created!");
			}
		}
	}

	private void deleteContent(File dir) {
		for (File file : dir.listFiles()) {
			if (file.isDirectory()) {
				deleteContent(file);
			}
			if (!file.delete()) {
				throw new RuntimeException(
						file.getAbsolutePath() + " could not be deleted!");
			}
		}
	}

	public void loadGraph(byte[][] args, int numberOfGraphChunks) {
		if (args.length < 4) {
			throw new IllegalArgumentException(
					"Loading a graph requires at least 4 arguments, but received only "
							+ args.length + " arguments.");
		}
		CoverStrategyType coverStrategy = CoverStrategyType.values()[ByteBuffer
				.wrap(args[0]).getInt()];
		int replicationPathLength = ByteBuffer.wrap(args[1]).getInt();
		int numberOfFiles = ByteBuffer.wrap(args[2]).getInt();
		loadGraph(coverStrategy, replicationPathLength, numberOfGraphChunks,
				numberOfFiles, getFileExtensions(args, 3));
	}

	private String[] getFileExtensions(byte[][] args, int startIndex) {
		String[] fileExtension = new String[args.length - startIndex];
		for (int i = 0; i < fileExtension.length; i++) {
			fileExtension[i] = MessageUtils
					.convertToString(args[startIndex + i], logger);
		}
		return fileExtension;
	}

	public void loadGraph(CoverStrategyType coverStrategy,
			int replicationPathLength, int numberOfGraphChunks,
			int numberOfFiles, String[] fileExtensions) {
		if (logger != null) {
			logger.finer("loadGraph(coverStrategy=" + coverStrategy.name()
					+ ", replicationPathLength=" + replicationPathLength
					+ ", numberOfFiles=" + numberOfFiles + ")");
		}
		this.coverStrategy = coverStrategy;
		this.replicationPathLength = replicationPathLength;
		this.numberOfGraphChunks = numberOfGraphChunks;
		fileReceiver = new FileReceiver(workingDir, clientId, clientConnections,
				numberOfFiles, fileExtensions, logger);
		fileReceiver.requestFiles();
	}

	public void receiveFileChunk(int fileID, long chunkID,
			long totalNumberOfChunks, byte[] chunkContent) {
		if (fileReceiver == null) {
			// this task has been closed
			return;
		}
		try {
			fileReceiver.receiveFileChunk(fileID, chunkID, totalNumberOfChunks,
					chunkContent);
			if (fileReceiver.isFinished()) {
				fileReceiver.close();
				fileReceiver = null;
				clientConnections.send(clientId,
						MessageUtils.createStringMessage(
								MessageType.MASTER_WORK_IN_PROGRESS,
								"Master received all files.", logger));
				start();
			} else if (clientConnections.isConnectionClosed(clientId)) {
				graphIsLoadingOrLoaded = false;
				close();
			}
		} catch (IOException e) {
			if (logger != null) {
				logger.throwing(e.getStackTrace()[0].getClassName(),
						e.getStackTrace()[0].getMethodName(), e);
			}
			clientConnections.send(clientId, MessageUtils.createStringMessage(
					MessageType.CLIENT_COMMAND_FAILED,
					e.getClass().getName() + ": " + e.getMessage(), logger));
			graphIsLoadingOrLoaded = false;
			close();
		}
	}

	@Override
	public void run() {
		try {
			keepAliveThread = new ClientConnectionKeepAliveTask(
					clientConnections, clientId);
			keepAliveThread.start();

			File[] chunks = createGraphChunks();
			File[] encodedFiles = encodeGraphFiles(chunks);
			// TODO in case of failure reset database
			keepAliveThread.interrupt();
			clientConnections.send(clientId, new byte[] {
					MessageType.CLIENT_COMMAND_SUCCEEDED.getValue() });
		} catch (Throwable e) {
			if (logger != null) {
				logger.throwing(e.getStackTrace()[0].getClassName(),
						e.getStackTrace()[0].getMethodName(), e);
			}
			clientConnections.send(clientId, MessageUtils.createStringMessage(
					MessageType.CLIENT_COMMAND_FAILED,
					e.getClass().getName() + ":" + e.getMessage(), logger));
			close();
		}
	}

	private File[] createGraphChunks() {
		if (logger != null) {
			logger.finer("creation of graph cover started");
		}
		clientConnections.send(clientId,
				MessageUtils.createStringMessage(
						MessageType.MASTER_WORK_IN_PROGRESS,
						"Started creation of graph cover.", logger));

		RDFFileIterator rdfFiles = new RDFFileIterator(workingDir, logger);
		GraphCoverCreator coverCreator = GraphCoverCreatorFactory
				.getGraphCoverCreator(coverStrategy, logger);
		File[] chunks = coverCreator.createGraphCover(rdfFiles, workingDir,
				numberOfGraphChunks);
		if (replicationPathLength != 0) {
			// TODO implement n-hop extension
		}

		if (logger != null) {
			logger.finer("creation of graph cover finished");
		}
		clientConnections.send(clientId,
				MessageUtils.createStringMessage(
						MessageType.MASTER_WORK_IN_PROGRESS,
						"Finished creation of graph cover.", logger));
		return chunks;
	}

	private File[] encodeGraphFiles(File[] plainGraphChunks) {
		if (logger != null) {
			logger.finer("encoding of graph chunks");
		}
		clientConnections.send(clientId,
				MessageUtils.createStringMessage(
						MessageType.MASTER_WORK_IN_PROGRESS,
						"Started encoding of graph chunks.", logger));
		File[] encodedFiles = new File[plainGraphChunks.length];
		// Dictionary dict = new Dictionary(logger);
		// for (int i = 0; i < plainFiles.length; i++) {
		// clientConnections.send(clientId,
		// MessageUtils.createStringMessage(
		// MessageType.MASTER_WORK_IN_PROGRESS,
		// "Started encoding of file " + i + ".", logger));
		// try {
		// encodedFiles[i] = dict.encode(plainFiles[i]);
		// } catch (RuntimeException e) {
		// clientConnections
		// .send(clientId,
		// MessageUtils.createStringMessage(
		// MessageType.MASTER_WORK_IN_PROGRESS,
		// "Error during encoding of file " + i
		// + ": " + e.getMessage(),
		// logger));
		// }
		// }
		if (logger != null) {
			logger.finer("encoding of graph chunks finished");
		}
		clientConnections.send(clientId,
				MessageUtils.createStringMessage(
						MessageType.MASTER_WORK_IN_PROGRESS,
						"Finished encoding of graph chunks.", logger));
		return encodedFiles;
	}

	public boolean isGraphLoadingOrLoaded() {
		return graphIsLoadingOrLoaded;
	}

	@Override
	public void close() {
		if (isAlive()) {
			interrupt();
			clientConnections.send(clientId,
					MessageUtils.createStringMessage(
							MessageType.CLIENT_COMMAND_FAILED,
							"GraphLoaderTask has been closed before it could finish.",
							logger));
			graphIsLoadingOrLoaded = false;
		}
		if (keepAliveThread != null && keepAliveThread.isAlive()) {
			keepAliveThread.interrupt();
		}
		if (fileReceiver != null) {
			fileReceiver.close();
			fileReceiver = null;
		}
		deleteContent(workingDir);
		workingDir.delete();
	}

}
