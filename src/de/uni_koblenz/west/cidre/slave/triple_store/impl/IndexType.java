package de.uni_koblenz.west.cidre.slave.triple_store.impl;

import java.nio.ByteBuffer;
import java.util.BitSet;

public enum IndexType {

	SPO {
		@Override
		public long getSubject(byte[] triple) {
			return ByteBuffer.wrap(triple, 0, 8).getLong();
		}

		@Override
		public long getProperty(byte[] triple) {
			return ByteBuffer.wrap(triple, 8, 16).getLong();
		}

		@Override
		public long getObject(byte[] triple) {
			return ByteBuffer.wrap(triple, 16, 24).getLong();
		}
	},

	OSP {
		@Override
		public long getSubject(byte[] triple) {
			return ByteBuffer.wrap(triple, 8, 16).getLong();
		}

		@Override
		public long getProperty(byte[] triple) {
			return ByteBuffer.wrap(triple, 16, 24).getLong();
		}

		@Override
		public long getObject(byte[] triple) {
			return ByteBuffer.wrap(triple, 0, 8).getLong();
		}
	},

	POS {
		@Override
		public long getSubject(byte[] triple) {
			return ByteBuffer.wrap(triple, 16, 24).getLong();
		}

		@Override
		public long getProperty(byte[] triple) {
			return ByteBuffer.wrap(triple, 0, 8).getLong();
		}

		@Override
		public long getObject(byte[] triple) {
			return ByteBuffer.wrap(triple, 8, 16).getLong();
		}
	};

	public abstract long getSubject(byte[] triple);

	public abstract long getProperty(byte[] triple);

	public abstract long getObject(byte[] triple);

	public BitSet getContainment(byte[] triple) {
		return BitSet.valueOf(ByteBuffer.wrap(triple, 24, triple.length - 24));
	}

}
