package de.uni_koblenz.west.cidre.common.query;

/**
 * Represents a triple pattern in SPARQL.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class TriplePattern {

  private final TriplePatternType type;

  private final long subject;

  private final long property;

  private final long object;

  public TriplePattern(TriplePatternType type, long subject, long property, long object) {
    this.type = type;
    this.subject = subject;
    this.property = property;
    this.object = object;
  }

  public TriplePatternType getType() {
    return type;
  }

  public boolean isSubjectVariable() {
    return type == TriplePatternType.___ || type == TriplePatternType.__O
            || type == TriplePatternType._P_ || type == TriplePatternType._PO;
  }

  public long getSubject() {
    return subject;
  }

  public boolean isPropertyVariable() {
    return type == TriplePatternType.___ || type == TriplePatternType.S__
            || type == TriplePatternType.__O || type == TriplePatternType.S_O;
  }

  public long getProperty() {
    return property;
  }

  public boolean isObjectVariable() {
    return type == TriplePatternType.___ || type == TriplePatternType.S__
            || type == TriplePatternType._P_ || type == TriplePatternType.SP_;
  }

  public long getObject() {
    return object;
  }

  public long[] getVariables() {
    switch (type) {
      case ___:
        return new long[] { subject, property, object };
      case S__:
        return new long[] { property, object };
      case _P_:
        return new long[] { subject, object };
      case __O:
        return new long[] { subject, property };
      case SP_:
        return new long[] { object };
      case S_O:
        return new long[] { property };
      case _PO:
        return new long[] { subject };
      case SPO:
      default:
        return new long[] {};
    }
  }

}
