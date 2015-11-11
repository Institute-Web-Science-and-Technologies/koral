package de.uni_koblenz.west.cidre.common.fileTransfer;

/**
 * Wraps one chunk of a file. Two chunks are ordered according to their position
 * in the file.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class FileChunk implements Comparable<FileChunk> {

	private final int fileID;

	private final long sequenceNumber;

	private long totalNumberOfSequences;

	private byte[] content;

	private long requestTime;

	public FileChunk(int fileID, long sequenceNumber) {
		this.fileID = fileID;
		this.sequenceNumber = sequenceNumber;
	}

	public FileChunk(int fileID, long sequenceNumber,
			long totalNumberOfSequences) {
		this(fileID, sequenceNumber);
		this.totalNumberOfSequences = totalNumberOfSequences;
	}

	public void setRequestTime(long requestTime) {
		this.requestTime = requestTime;
	}

	public long getRequestTime() {
		return requestTime;
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
		return totalNumberOfSequences > 0
				&& sequenceNumber >= totalNumberOfSequences - 1;
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

	@Override
	public String toString() {
		return "FileChunk [fileID=" + fileID + ", sequenceNumber="
				+ sequenceNumber + "/" + (totalNumberOfSequences - 1) + "]";
	}

}
