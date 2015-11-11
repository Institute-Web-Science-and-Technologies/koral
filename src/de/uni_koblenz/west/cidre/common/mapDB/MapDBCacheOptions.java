package de.uni_koblenz.west.cidre.common.mapDB;

import org.mapdb.DBMaker;

/**
 * Options to configure the used instance cache of MapDB.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public enum MapDBCacheOptions {

	NONE {
		@Override
		public DBMaker<?> setCaching(DBMaker<?> dbmaker) {
			return dbmaker.cacheDisable();
		}
	},

	HASH_TABLE {
		@Override
		public DBMaker<?> setCaching(DBMaker<?> dbmaker) {
			return dbmaker;
		}
	},

	LEAST_RECENTLY_USED {
		@Override
		public DBMaker<?> setCaching(DBMaker<?> dbmaker) {
			return dbmaker.cacheLRUEnable();
		}
	},

	HARD_REFERENCE {
		@Override
		public DBMaker<?> setCaching(DBMaker<?> dbmaker) {
			return dbmaker.cacheHardRefEnable();
		}
	},

	SOFT_REFERENCE {
		@Override
		public DBMaker<?> setCaching(DBMaker<?> dbmaker) {
			return dbmaker.cacheSoftRefEnable();
		}
	},

	WEAK_REFERENCE {
		@Override
		public DBMaker<?> setCaching(DBMaker<?> dbmaker) {
			return dbmaker.cacheWeakRefEnable();
		}
	};

	public abstract DBMaker<?> setCaching(DBMaker<?> dbmaker);

}
