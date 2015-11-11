package de.uni_koblenz.west.cidre.common.query;

/**
 * Represents a triple pattern in SPARQL.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class TriplePattern {

	private final TriplePatternType type;

	public TriplePattern() {
		// TODO Auto-generated constructor stub
		type = TriplePatternType.___;
	}

	public TriplePatternType getType() {
		return type;
	}

	public long getSubject() {
		// TODO auto-generated method stub
		return 0l;
	}

	public long getProperty() {
		// TODO auto-generated method stub
		return 0l;
	}

	public long getObject() {
		// TODO auto-generated method stub
		return 0l;
	}

}
