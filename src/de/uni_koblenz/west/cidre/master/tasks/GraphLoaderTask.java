package de.uni_koblenz.west.cidre.master.tasks;

import java.io.Closeable;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

import de.uni_koblenz.west.cidre.common.messages.MessageType;
import de.uni_koblenz.west.cidre.master.client_manager.ClientConnectionManager;
import de.uni_koblenz.west.cidre.master.graph_cover_creator.CoverStrategyType;

public class GraphLoaderTask extends Thread implements Closeable {

	private final Logger logger;

	private final int clientId;

	private final ClientConnectionManager clientConnections;

	private final File workingDir;

	private CoverStrategyType coverStrategy;

	private int replicationPathLength;

	private int numberOfFiles;

	public GraphLoaderTask(int clientID,
			ClientConnectionManager clientConnections, File tmpDir,
			Logger logger) {
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

	public void loadGraph(byte[][] args) {
		if (args.length != 3) {
			throw new IllegalArgumentException(
					"Loading a graph requires 3 arguments, but received only "
							+ args.length + " arguments.");
		}
		CoverStrategyType coverStrategy = CoverStrategyType.values()[ByteBuffer
				.wrap(args[0]).getInt()];
		int replicationPathLength = ByteBuffer.wrap(args[1]).getInt();
		int numberOfFiles = ByteBuffer.wrap(args[2]).getInt();
		loadGraph(coverStrategy, replicationPathLength, numberOfFiles);
	}

	public void loadGraph(CoverStrategyType coverStrategy,
			int replicationPathLength, int numberOfFiles) {
		if (logger != null) {
			logger.finer("loadGraph(coverStrategy=" + coverStrategy.name()
					+ ", replicationPathLength=" + replicationPathLength
					+ ", numberOfFiles=" + numberOfFiles + ")");
		}
		this.coverStrategy = coverStrategy;
		this.replicationPathLength = replicationPathLength;
		this.numberOfFiles = numberOfFiles;
		start();
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		// TODO Server may only load a graph once
		clientConnections.send(clientId,
				new byte[] { MessageType.CLIENT_COMMAND_SUCCEEDED.getValue() });
	}

	@Override
	public void close() {
		deleteContent(workingDir);
		workingDir.delete();
	}

}
