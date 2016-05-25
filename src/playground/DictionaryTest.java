package playground;

import org.apache.jena.graph.Node;

import de.uni_koblenz.west.koral.common.utils.RDFFileIterator;
import de.uni_koblenz.west.koral.master.dictionary.Dictionary;
import de.uni_koblenz.west.koral.master.dictionary.impl.RocksDBDictionary;
import de.uni_koblenz.west.koral.master.utils.DeSerializer;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Tests different dictionary implementations.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class DictionaryTest {

  public static void main(String[] args) {
    if (args.length != 2) {
      System.out.println("Usage: java " + DictionaryTest.class.getName()
              + " <storageDir> <graphDataFileOrFolder>");
      return;
    }
    Dictionary dictionary = new RocksDBDictionary(args[0]);
    // Dictionary dictionary = new LevelDBDictionary(args[0]);
    // Dictionary dictionary = new
    // MapDBDictionary(MapDBStorageOptions.MEMORY_MAPPED_FILE,
    // MapDBDataStructureOptions.HASH_TREE_MAP, args[0], false, true,
    // MapDBCacheOptions.HASH_TABLE);
    DictionaryTest.encode(args, dictionary);
    dictionary.close();
  }

  private static void encode(String[] args, Dictionary dictionary) {
    long start = System.currentTimeMillis();
    try (RDFFileIterator iter = new RDFFileIterator(new File(args[1]), false, null);) {
      long numberOfTriples = 0;
      SimpleDateFormat format = new SimpleDateFormat();
      for (Node[] quad : iter) {
        dictionary.encode(DeSerializer.serializeNode(quad[0]), true);
        dictionary.encode(DeSerializer.serializeNode(quad[1]), true);
        dictionary.encode(DeSerializer.serializeNode(quad[2]), true);
        numberOfTriples++;
        if ((numberOfTriples % 1_000_000) == 0) {
          System.out.println(format.format(new Date(System.currentTimeMillis())) + " "
                  + (numberOfTriples / 1_000_000) + "M triples");
        }
      }
    }
    dictionary.flush();
    System.out.println("duration: " + (System.currentTimeMillis() - start) + " msec");
  }

}
