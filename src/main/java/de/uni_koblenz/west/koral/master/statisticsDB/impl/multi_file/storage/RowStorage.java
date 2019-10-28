package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage.shared_space.SharedSpaceConsumer;

/**
 * Describes a storage implementation for rows of the statistics database.
 *
 * @author Philipp Töws
 *
 */
public interface RowStorage extends AutoCloseable, SharedSpaceConsumer {

	/**
	 * Prepares the storage for further accesses. Can be called after close() to reopen. Throws exception if
	 * createIfNotExisting is false and file doesn't exist.
	 *
	 * @param createIfNotExisting
	 */
	public void open(boolean createIfNotExisting);

	/**
	 *
	 * @param rowId
	 * @return Null if no entry found
	 * @throws IOException
	 */
	public byte[] readRow(long rowId) throws IOException;

	/**
	 * @return False if there is not enough space left, true otherwise.
	 */
	public boolean writeRow(long rowId, byte[] row) throws IOException;

	/**
	 * @return All rows contained in the storage.
	 */
	public Iterator<Entry<Long, byte[]>> getBlockIterator() throws IOException;

	/**
	 * Store the given blocks. It must be ensured that the data is persisted if possible, i.e. no flush is required
	 * afterwards. Note that some implementations may only store a fixed size of each block (the rest is assumed to be
	 * padding/metadata).
	 *
	 * @param blocks
	 * @throws IOException
	 */
	public void storeBlocks(Iterator<Entry<Long, byte[]>> blocks) throws IOException;

	/**
	 * If this method returns false, storage must be reopened
	 *
	 * @return
	 */
	public boolean valid();

	/**
	 * If no useful information is contained, so that the storage can be cleaned up/deleted.
	 *
	 * @return
	 */
	public boolean isEmpty();

	/**
	 * @return The used space that is needed to persist the data as is (might include padding/metadata).
	 */
	public long length();

	public int getRowLength();

	public void flush() throws IOException;

	public void delete();

	@Override
	public void close();
}
