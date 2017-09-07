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
    return pathname.exists() && pathname.isFile() && (lang != null);
  }

}
