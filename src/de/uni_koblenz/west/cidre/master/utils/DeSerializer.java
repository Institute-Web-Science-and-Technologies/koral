package de.uni_koblenz.west.cidre.master.utils;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.riot.out.NodeFmtLib;
import org.apache.jena.riot.tokens.Token;
import org.apache.jena.riot.tokens.Tokenizer;
import org.apache.jena.riot.tokens.TokenizerFactory;

import java.util.BitSet;
import java.util.regex.Pattern;

/**
 * Provides convenient methods to serialize {@link Node}s and {@link BitSet}s as
 * {@link String}s and to deserialize them again.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class DeSerializer {

  public static String serializeNode(Node node) {
    return NodeFmtLib.str(node);
  }

  public static Node deserializeNode(String serializedNode) {
    Tokenizer tokenizer = TokenizerFactory.makeTokenizerString(serializedNode);
    while (tokenizer.hasNext()) {
      Token t = tokenizer.next();
      return t.asNode();
    }
    throw new RuntimeException("decoding of " + serializedNode + " failed");
  }

  public static Node serializeBitSetAsNode(byte[] bitset) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bitset) {
      sb.append(":").append(b);
    }
    return NodeFactory.createURI("urn:containment" + sb.toString());
  }

  public static byte[] deserializeBitSetFromNode(Node bitsetNode) {
    String stringSerialization = bitsetNode.getURI();
    String[] parts = stringSerialization.split(Pattern.quote(":"));
    byte[] bitSetBytes = new byte[parts.length - 2];
    for (int i = 2; i < parts.length; i++) {
      bitSetBytes[i - 2] = Byte.parseByte(parts[i]);
    }
    return bitSetBytes;
  }

}
