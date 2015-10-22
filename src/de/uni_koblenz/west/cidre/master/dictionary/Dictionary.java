package de.uni_koblenz.west.cidre.master.dictionary;

import java.io.Closeable;

public interface Dictionary extends Closeable {

	/**
	 * if value already exists, its id is returned. Otherwise a new id is
	 * generated
	 * 
	 * @param value
	 * @return
	 */
	public long encode(String value);

	/**
	 * @param id
	 * @return <code>null</code> if no String has been encoded to this id, yet.
	 */
	public String decode(long id);

}
