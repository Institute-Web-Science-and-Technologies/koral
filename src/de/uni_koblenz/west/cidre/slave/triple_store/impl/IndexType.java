package de.uni_koblenz.west.cidre.slave.triple_store.impl;

import java.nio.ByteBuffer;
import java.util.BitSet;

import de.uni_koblenz.west.cidre.common.utils.NumberConversion;

public enum IndexType {

	SPO {
		@Override
		public long getSubject(byte[] triple) {
			return NumberConversion.bytes2long(triple, 0);
		}

		@Override
		public long getProperty(byte[] triple) {
			return NumberConversion.bytes2long(triple, 8);
		}

		@Override
		public long getObject(byte[] triple) {
			return NumberConversion.bytes2long(triple, 16);
		}
	},

	OSP {
		@Override
		public long getSubject(byte[] triple) {
			return NumberConversion.bytes2long(triple, 8);
		}

		@Override
		public long getProperty(byte[] triple) {
			return NumberConversion.bytes2long(triple, 16);
		}

		@Override
		public long getObject(byte[] triple) {
			return NumberConversion.bytes2long(triple, 0);
		}
	},

	POS {
		@Override
		public long getSubject(byte[] triple) {
			return NumberConversion.bytes2long(triple, 16);
		}

		@Override
		public long getProperty(byte[] triple) {
			return NumberConversion.bytes2long(triple, 0);
		}

		@Override
		public long getObject(byte[] triple) {
			return NumberConversion.bytes2long(triple, 8);
		}
	};

	public abstract long getSubject(byte[] triple);

	public abstract long getProperty(byte[] triple);

	public abstract long getObject(byte[] triple);

	public BitSet getContainment(byte[] triple) {
		return BitSet.valueOf(ByteBuffer.wrap(triple, 24, triple.length - 24));
	}

}
