package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

import java.io.IOException;

/**
 * Interface of an extra row storage/extra file.
 *
 * @author Philipp Töws
 *
 */
public interface ExtraRowStorage extends RowStorage {

	public long writeRow(byte[] row) throws IOException;

	public void deleteRow(long rowId);

	public long[] getFreeSpaceIndexData();

	public void defragFreeSpaceIndex();

}
