package de.uni_koblenz.west.cidre.master.dictionary;

import java.io.Closeable;

public interface Dictionary extends Closeable {

	/**
	 * if value already exists, its id is returned. Otherwise a new id is
	 * generated whose first two bytes are 0.
	 * 
	 * @param value
	 * @return
	 * @throws RuntimeException
	 *             if maximum number of strings (i.e., 2^48) have been encoded
	 */
	public long encode(String value);

	/**
	 * the same as <code>setOwner(encode(value), owner)</code>
	 * 
	 * @param value
	 * @param owner
	 * @return
	 */
	public long setOwner(String value, short owner);

	/**
	 * updates the dictionary such that the first two bytes of id is set to
	 * owner.
	 * 
	 * @param value
	 * @param owner
	 * @return
	 * @throws IllegalArgumentException
	 *             if the first two bytes of id are not 0
	 */
	public long setOwner(long id, short owner);

	/**
	 * @param id
	 * @return <code>null</code> if no String has been encoded to this id, yet.
	 */
	public String decode(long id);

	public void clear();

	@Override
	public void close();

}
