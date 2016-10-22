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
package playground;

import de.uni_koblenz.west.koral.common.mapDB.MapDBCacheOptions;
import de.uni_koblenz.west.koral.common.mapDB.MapDBStorageOptions;
import de.uni_koblenz.west.koral.common.query.Mapping;
import de.uni_koblenz.west.koral.common.query.MappingRecycleCache;
import de.uni_koblenz.west.koral.common.query.TriplePattern;
import de.uni_koblenz.west.koral.common.query.TriplePatternType;
import de.uni_koblenz.west.koral.common.utils.JoinMappingCache;
import de.uni_koblenz.west.koral.common.utils.MapDBJoinMappingCache;
import de.uni_koblenz.west.koral.common.utils.MappingIteratorWrapper;
import de.uni_koblenz.west.koral.common.utils.NumberConversion;
import de.uni_koblenz.west.koral.slave.triple_store.impl.IndexType;

import java.io.File;

public class JoinCacheTest {

  public static void main(String[] args) {
    MappingRecycleCache recycleCache = new MappingRecycleCache(10, 4);
    JoinMappingCache cache = new MapDBJoinMappingCache(MapDBStorageOptions.MEMORY_MAPPED_FILE,
            false, true, MapDBCacheOptions.HASH_TABLE, new File("/tmp"), recycleCache, "test",
            new long[] { 0, 1, 2 }, new int[] { 1, 2, 0 }, 0);
    TriplePattern triplePattern = new TriplePattern(TriplePatternType.___, 0, 1, 2);
    for (long s = 0; s < 10; s++) {
      for (long p = 0; p < 10; p++) {
        for (long o = 0; o < 10; o++) {
          byte[] triple = new byte[(Long.BYTES * 3) + 1];
          NumberConversion.long2bytes(s, triple, 0 * Long.BYTES);
          NumberConversion.long2bytes(p, triple, 1 * Long.BYTES);
          NumberConversion.long2bytes(o, triple, 2 * Long.BYTES);
          Mapping mapping = recycleCache.createMapping(triplePattern, IndexType.SPO, triple);
          mapping.updateContainment(0, 1);
          cache.add(mapping);
        }
      }
    }

    byte[] triple = new byte[(Long.BYTES * 3) + 1];
    NumberConversion.long2bytes(0, triple, 0 * Long.BYTES);
    NumberConversion.long2bytes(5, triple, 1 * Long.BYTES);
    NumberConversion.long2bytes(1, triple, 2 * Long.BYTES);
    TriplePattern pattern = new TriplePattern(TriplePatternType.SPO, 0, 1, 2);
    Mapping joinMapping = recycleCache.createMapping(pattern, IndexType.SPO, triple);
    joinMapping.updateContainment(0, 1);

    long[] resultVars = new long[] { 0, 1, 2 };
    for (Mapping m : (MappingIteratorWrapper) cache.getMatchCandidates(joinMapping,
            new long[] {})) {
      System.out.println(m.toString(resultVars));
    }

    cache.close();
  }

}
