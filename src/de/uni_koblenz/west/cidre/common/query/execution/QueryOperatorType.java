package de.uni_koblenz.west.cidre.common.query.execution;

public enum QueryOperatorType {

  PROJECTION, TRIPLE_PATTERN_JOIN, TRIPLE_PATTERN_MATCH;

  public static QueryOperatorType valueOf(int operatorType) {
    QueryOperatorType[] operatorTypes = values();
    if (operatorType < operatorTypes.length) {
      return operatorTypes[operatorType];
    }
    throw new IllegalArgumentException(
            "There does not exist a query operator of type " + operatorType + ".");
  }

}
