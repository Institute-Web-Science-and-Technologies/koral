package de.uni_koblenz.west.cidre.common.utils;

import java.io.File;
import java.io.FileFilter;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;

public class GraphFileFilter implements FileFilter {

	@Override
	public boolean accept(File pathname) {
		Lang lang = RDFLanguages.filenameToLang(pathname.getName());
		return pathname.exists() && pathname.isFile() && lang != null;
	}

}
