package de.uni_koblenz.west.koral.master.statisticsDB.impl.multi_file.storage;

import java.util.Map;

/**
 * Used for transferring pairs of block id and block data between storage implementations.
 *
 * @author Philipp Töws
 *
 */
class BlockEntry implements Map.Entry<Long, byte[]> {

	private long blockId;

	private byte[] block;

	private static BlockEntry blockEntry;

	private BlockEntry() {}

	public static BlockEntry getInstance(long blockId, byte[] block) {
		if (blockEntry == null) {
			blockEntry = new BlockEntry();
		}
		blockEntry.blockId = blockId;
		blockEntry.block = block;
		return blockEntry;
	}

	@Override
	public Long getKey() {
		return blockId;
	}

	@Override
	public byte[] getValue() {
		return block;
	}

	@Override
	public byte[] setValue(byte[] value) {
		throw new UnsupportedOperationException();
	}

}
