package de.uni_koblenz.west.cidre.master.tasks;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.logging.Logger;

import de.uni_koblenz.west.cidre.common.fileTransfer.FileReceiver;
import de.uni_koblenz.west.cidre.common.fileTransfer.FileSenderConnection;
import de.uni_koblenz.west.cidre.common.messages.MessageType;
import de.uni_koblenz.west.cidre.common.messages.MessageUtils;
import de.uni_koblenz.west.cidre.common.networManager.MessageNotifier;
import de.uni_koblenz.west.cidre.common.utils.NumberConversion;
import de.uni_koblenz.west.cidre.common.utils.RDFFileIterator;
import de.uni_koblenz.west.cidre.master.client_manager.ClientConnectionManager;
import de.uni_koblenz.west.cidre.master.dictionary.DictionaryEncoder;
import de.uni_koblenz.west.cidre.master.graph_cover_creator.CoverStrategyType;
import de.uni_koblenz.west.cidre.master.graph_cover_creator.GraphCoverCreator;
import de.uni_koblenz.west.cidre.master.graph_cover_creator.GraphCoverCreatorFactory;
import de.uni_koblenz.west.cidre.master.networkManager.FileChunkRequestListener;
import de.uni_koblenz.west.cidre.master.networkManager.impl.FileChunkRequestProcessor;
import de.uni_koblenz.west.cidre.master.statisticsDB.GraphStatistics;

public class GraphLoaderTask extends Thread implements Closeable {

	private final Logger logger;

	private final int clientId;

	private final ClientConnectionManager clientConnections;

	private final DictionaryEncoder dictionary;

	private final GraphStatistics statistics;

	private final File workingDir;

	private CoverStrategyType coverStrategy;

	private int replicationPathLength;

	private int numberOfGraphChunks;

	private FileReceiver fileReceiver;

	private ClientConnectionKeepAliveTask keepAliveThread;

	private boolean graphIsLoadingOrLoaded;

	private final MessageNotifier messageNotifier;

	private FileSenderConnection fileSenderConnection;

	private boolean isStarted;

	public GraphLoaderTask(int clientID,
			ClientConnectionManager clientConnections,
			DictionaryEncoder dictionary, GraphStatistics statistics,
			File tmpDir, MessageNotifier messageNotifier, Logger logger) {
		isDaemon();
		graphIsLoadingOrLoaded = true;
		isStarted = false;
		clientId = clientID;
		this.clientConnections = clientConnections;
		this.dictionary = dictionary;
		this.statistics = statistics;
		this.messageNotifier = messageNotifier;
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
		if (dir.exists()) {
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
	}

	public void loadGraph(byte[][] args, int numberOfGraphChunks,
			FileSenderConnection fileSenderConnection) {
		if (args.length < 4) {
			throw new IllegalArgumentException(
					"Loading a graph requires at least 4 arguments, but received only "
							+ args.length + " arguments.");
		}
		CoverStrategyType coverStrategy = CoverStrategyType
				.values()[NumberConversion.bytes2int(args[0])];
		int replicationPathLength = NumberConversion.bytes2int(args[1]);
		int numberOfFiles = NumberConversion.bytes2int(args[2]);
		loadGraph(coverStrategy, replicationPathLength, numberOfGraphChunks,
				numberOfFiles, getFileExtensions(args, 3),
				fileSenderConnection);
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
			int numberOfFiles, String[] fileExtensions,
			FileSenderConnection fileSenderConnection) {
		if (logger != null) {
			logger.finer("loadGraph(coverStrategy=" + coverStrategy.name()
					+ ", replicationPathLength=" + replicationPathLength
					+ ", numberOfFiles=" + numberOfFiles + ")");
		}
		this.coverStrategy = coverStrategy;
		this.replicationPathLength = replicationPathLength;
		this.numberOfGraphChunks = numberOfGraphChunks;
		this.fileSenderConnection = fileSenderConnection;
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
		isStarted = true;
		try {
			keepAliveThread = new ClientConnectionKeepAliveTask(
					clientConnections, clientId);
			keepAliveThread.start();

			File[] chunks = createGraphChunks();
			File[] encodedFiles = encodeGraphFiles(chunks);

			List<FileChunkRequestProcessor> fileSenders = new LinkedList<>();
			for (int i = 0; i < encodedFiles.length; i++) {
				File file = encodedFiles[i];
				if (file == null) {
					continue;
				}
				// slave ids start with 1!
				FileChunkRequestProcessor sender = new FileChunkRequestProcessor(
						i + 1, logger);
				fileSenders.add(sender);
				messageNotifier.registerMessageListener(
						FileChunkRequestListener.class, sender);
				sender.sendFile(file, fileSenderConnection);
			}

			while (!isInterrupted() && !fileSenders.isEmpty()) {
				long currentTime = System.currentTimeMillis();
				ListIterator<FileChunkRequestProcessor> iterator = fileSenders
						.listIterator();
				try {
					while (!isInterrupted() && iterator.hasNext()) {
						FileChunkRequestProcessor sender = iterator.next();
						if (sender.isFinished()) {
							messageNotifier.unregisterMessageListener(
									FileChunkRequestListener.class, sender);
							sender.close();
							iterator.remove();
						} else if (sender.isFailed()) {
							throw new RuntimeException(
									sender.getErrorMessage());
						}
					}
				} catch (RuntimeException e) {
					// clean up listeners
					iterator = fileSenders.listIterator();
					while (iterator.hasNext()) {
						FileChunkRequestProcessor sender = iterator.next();
						messageNotifier.unregisterMessageListener(
								FileChunkRequestListener.class, sender);
						sender.close();
						iterator.remove();
					}
					throw e;
				}
				long timeToSleep = 100
						- (System.currentTimeMillis() - currentTime);
				if (!isInterrupted() && timeToSleep > 0) {
					try {
						sleep(timeToSleep);
					} catch (InterruptedException e) {
						break;
					}
				}
			}

			keepAliveThread.interrupt();

			if (fileSenders.isEmpty()) {
				clientConnections.send(clientId, new byte[] {
						MessageType.CLIENT_COMMAND_SUCCEEDED.getValue() });
			} else {
				clientConnections.send(clientId,
						MessageUtils.createStringMessage(
								MessageType.CLIENT_COMMAND_FAILED,
								"Loading of graph was interrupted before all slaves have loaded the graph.",
								logger));
			}
		} catch (Throwable e) {
			clearDatabase();
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

	private void clearDatabase() {
		dictionary.clear();
		statistics.clear();
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
		File[] encodedFiles = dictionary.encodeGraphChunks(plainGraphChunks,
				statistics, workingDir);
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
		} else if (!isStarted) {
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
