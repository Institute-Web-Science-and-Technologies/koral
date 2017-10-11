/*
 * This file is part of Koral.
 *
 * Koral is free software: you can redistribute it and/or modify it under the terms of the GNU
 * Lesser General Public License as published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Koral is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even
 * the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
 * General Public License for more details.
 *
 * You should have received a copy of the GNU Leser General Public License along with Koral. If not,
 * see <http://www.gnu.org/licenses/>.
 *
 * Copyright 2016 Daniel Janke
 */
package playground;

import org.apache.jena.graph.Node;

import de.uni_koblenz.west.koral.common.config.impl.Configuration;
import de.uni_koblenz.west.koral.common.io.EncodingFileFormat;
import de.uni_koblenz.west.koral.common.query.Mapping;
import de.uni_koblenz.west.koral.common.query.MappingRecycleCache;
import de.uni_koblenz.west.koral.common.query.TriplePattern;
import de.uni_koblenz.west.koral.common.query.TriplePatternType;
import de.uni_koblenz.west.koral.common.utils.GraphFileFilter;
import de.uni_koblenz.west.koral.master.dictionary.DictionaryEncoder;
import de.uni_koblenz.west.koral.master.utils.DeSerializer;
import de.uni_koblenz.west.koral.slave.triple_store.TripleStoreAccessor;

import java.io.File;

/**
 * A class to test source code. Not used within Koral.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class Minimal {

  public static void main(String[] args) {
    if (args.length == 0) {
      System.out.println("Missing input file");
      return;
    }
    File workingDir = new File(System.getProperty("java.io.tmpdir") + File.separator + "koralTest");
    if (!workingDir.exists()) {
      workingDir.mkdir();
    }

    File inputFile = new File(args[0]);
    Configuration conf = new Configuration();
    conf.addSlave("127.0.0.1");

    // encode graph
    DictionaryEncoder encoder = new DictionaryEncoder(conf, null, null);
    File encodedInput = encoder.encodeOriginalGraphFiles(
            inputFile.isDirectory() ? inputFile.listFiles(new GraphFileFilter())
                    : new File[] { inputFile },
            workingDir, EncodingFileFormat.EEE, 1);

    // store triples
    TripleStoreAccessor accessor = new TripleStoreAccessor(conf, null);
    accessor.storeTriples(encodedInput);

    long[] vars = new long[] { 1, 2, 3 };
    for (Mapping m : accessor.lookup(new MappingRecycleCache(10, 1),
            new TriplePattern(TriplePatternType.___, 1, 2, 3))) {
      String delim = "";
      for (long var : vars) {
        long value = m.getValue(var, vars);
        Node identifier = encoder.decode(value);
        System.out.print(delim + DeSerializer.serializeNode(identifier));
        delim = " ";
      }
      System.out.print("\n");
    }

    encoder.close();
    accessor.close();

    Minimal.delete(workingDir);
  }

  static void delete(File dir) {
    for (File file : dir.listFiles()) {
      if (file.isDirectory()) {
        Minimal.delete(file);
      } else {
        file.delete();
      }
    }
    dir.delete();
  }

}
