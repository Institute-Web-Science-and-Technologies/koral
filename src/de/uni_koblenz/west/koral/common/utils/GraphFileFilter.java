package de.uni_koblenz.west.koral.common.utils;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;

import java.io.File;
import java.io.FileFilter;

/**
 * Filters all files in a directory, whether they contain a loadable graph or
 * not.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
public class GraphFileFilter implements FileFilter {

  @Override
  public boolean accept(File pathname) {
    Lang lang = RDFLanguages.filenameToLang(pathname.getName());
    return pathname.exists() && pathname.isFile() && lang != null;
  }

}
