package de.uni_koblenz.west.cidre.master.dictionary.impl;

import java.io.File;

import org.mapdb.DBMaker;

public enum MapDBStorageOptions {

	RANDOM_ACCESS_FILE {
		@Override
		public DBMaker<?> getDBMaker(String file) {
			return DBMaker.newFileDB(new File(file));
		}
	},

	MEMORY_MAPPED_FILE {
		@Override
		public DBMaker<?> getDBMaker(String file) {
			return DBMaker.newFileDB(new File(file))
					.mmapFileEnableIfSupported();
		}
	},

	MEMORY {
		@Override
		public DBMaker<?> getDBMaker(String file) {
			return DBMaker.newMemoryDB();
		}
	};

	public abstract DBMaker<?> getDBMaker(String file);

}
