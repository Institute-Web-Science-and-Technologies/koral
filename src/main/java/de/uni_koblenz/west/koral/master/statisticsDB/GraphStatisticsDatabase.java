/*
 * This file is part of Koral.
 *
 * Koral is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Koral is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Leser General Public License
 * along with Koral.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright 2016 Daniel Janke
 */
package de.uni_koblenz.west.koral.master.statisticsDB;

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

  public void incrementNumberOfTriplesPerChunk(int chunk);

  public long[] getChunkSizes();

  /**
   * @param id
   * @return long[] first numberOfChunks indices represent how often resource id
   *         occurs as subject; second numberOfChunks indices represent how
   *         often resource id occurs as property; third numberOfChunks indices
   *         represent how often resource id occurs as object; last index
   *         represents the overall number of occurrences of resource id
   */
  public long[] getStatisticsForResource(long id);

  public void clear();

  @Override
  public void close();

}
