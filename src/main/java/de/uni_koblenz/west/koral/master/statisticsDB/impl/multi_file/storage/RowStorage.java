package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

import java.io.IOException;

public interface RowStorage extends AutoCloseable {

	public void open(boolean createIfNotExisting);

	public byte[] readRow(long rowId) throws IOException;

	public void writeRow(long rowId, byte[] row) throws IOException;

	public boolean valid();

	public long length();

	public void delete();

	@Override
	public void close();
}
