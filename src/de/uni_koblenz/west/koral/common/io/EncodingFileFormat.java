package de.uni_koblenz.west.koral.common.io;

/**
 * Defines which elements of a triple are already encoded. U stands for
 * unencoded, E stands for encoded.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public enum EncodingFileFormat {

  UUU, EUU, UEU, UUE, EEU, EUE, UEE, EEE;

  public boolean isSubjectEncoded() {
    return (this == EUU) || (this == EEU) || (this == EUE) || (this == EEE);
  }

  public boolean isPropertyEncoded() {
    return (this == UEU) || (this == EEU) || (this == UEE) || (this == EEE);
  }

  public boolean isObjectEncoded() {
    return (this == UUE) || (this == EUE) || (this == UEE) || (this == EEE);
  }

}
