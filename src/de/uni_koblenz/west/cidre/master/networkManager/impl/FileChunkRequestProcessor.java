package de.uni_koblenz.west.cidre.master.networkManager.impl;

import java.io.File;

import de.uni_koblenz.west.cidre.master.networkManager.FileChunkRequestListener;

public class FileChunkRequestProcessor implements FileChunkRequestListener {

	public FileChunkRequestProcessor(int slaveID) {
		// TODO Auto-generated constructor stub
	}

	@Override
	public int getSlaveID() {
		// TODO Auto-generated method stub
		return 0;
	}

	public void sendFile(File file) {
		// TODO Auto-generated method stub

	}

	@Override
	public void processMessage(byte[][] message) {
		// TODO Auto-generated method stub

	}

	public boolean isFinished() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

}
