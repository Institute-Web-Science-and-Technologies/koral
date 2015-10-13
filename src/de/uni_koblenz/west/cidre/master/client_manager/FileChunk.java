package de.uni_koblenz.west.cidre.master.client_manager;

class FileChunk implements Comparable<FileChunk> {

	private final int fileID;

	private final long sequenceNumber;

	private long totalNumberOfSequences;

	private byte[] content;

	public FileChunk(int fileID, long sequenceNumber) {
		this.fileID = fileID;
		this.sequenceNumber = sequenceNumber;
	}

	public FileChunk(int fileID, long sequenceNumber,
			long totalNumberOfSequences) {
		this(fileID, sequenceNumber);
		this.totalNumberOfSequences = totalNumberOfSequences;
	}

	public int getFileID() {
		return fileID;
	}

	public long getSequenceNumber() {
		return sequenceNumber;
	}

	public long getTotalNumberOfSequences() {
		return totalNumberOfSequences;
	}

	public void setTotalNumberOfSequences(long totalNumberOfSequences) {
		this.totalNumberOfSequences = totalNumberOfSequences;
	}

	public boolean isReceived() {
		return content != null;
	}

	public void setContent(byte[] content) {
		this.content = content;
	}

	public boolean isLastChunk() {
		return content != null && sequenceNumber >= totalNumberOfSequences - 1;
	}

	public byte[] getContent() {
		return content;
	}

	@Override
	public int compareTo(FileChunk o) {
		int diff = fileID - o.fileID;
		if (diff != 0) {
			return diff;
		}
		return (int) (sequenceNumber - o.sequenceNumber);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + fileID;
		result = prime * result
				+ (int) (sequenceNumber ^ (sequenceNumber >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		FileChunk other = (FileChunk) obj;
		if (fileID != other.fileID) {
			return false;
		}
		if (sequenceNumber != other.sequenceNumber) {
			return false;
		}
		return true;
	}

}
