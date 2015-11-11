package de.uni_koblenz.west.cidre.master.statisticsDB;

import java.io.Closeable;

/**
 * Methods required by {@link GraphStatistics} to persist the statistical
 * information.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public interface GraphStatisticsDatabase extends Closeable {

	public void incrementSubjectCount(long subject, int chunk);

	public void incrementPropertyCount(long property, int chunk);

	public void incrementObjectCount(long object, int chunk);

	public void incrementRessourceOccurrences(long resource, int chunk);

	public void incrementNumberOfTriplesPerChunk(int chunk);

	public long[] getChunkSizes();

	/**
	 * @param id
	 * @return long[] first numberOfChunks indices represent how often resource
	 *         id occurs as subject; second numberOfChunks indices represent how
	 *         often resource id occurs as property; third numberOfChunks
	 *         indices represent how often resource id occurs as object; last
	 *         index represents the overall number of occurrences of resource id
	 */
	public long[] getStatisticsForResource(long id);

	public long[] getOwnerLoad();

	/**
	 * updates the dictionary such that the first two bytes of id is set to
	 * owner.
	 * 
	 * @param id
	 * @param owner
	 * @return
	 * @throws IllegalArgumentException
	 *             if the first two bytes of id are not 0 or not equal to owner
	 */
	public long setOwner(long id, short owner);

	public void clear();

	@Override
	public void close();

}
