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
package de.uni_koblenz.west.koral.master.utils;

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
