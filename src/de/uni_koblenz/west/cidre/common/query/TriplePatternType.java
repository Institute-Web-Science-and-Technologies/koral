package de.uni_koblenz.west.cidre.common.query;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

/**
 * This class defines the different type of triple pattern known by SPARQL.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public enum TriplePatternType {

	___, S__, _P_, __O, SP_, S_O, _PO, SPO;

	public static TriplePatternType valueOf(int value) {
		for (TriplePatternType type : values()) {
			if (type.ordinal() == value) {
				return type;
			}
		}
		throw new IllegalArgumentException(
				"Unknown " + TriplePattern.class.getName()
						+ " constant with octal value " + value + ".");
	}

	public static TriplePatternType valueOf(Triple triple) {
		Node subject = triple.getSubject();
		Node property = triple.getPredicate();
		Node object = triple.getObject();
		String pattern = (subject.isVariable() ? "_" : "S")
				+ (property.isVariable() ? "_" : "P")
				+ (object.isVariable() ? "_" : "O");
		return TriplePatternType.valueOf(pattern);
	}

}
