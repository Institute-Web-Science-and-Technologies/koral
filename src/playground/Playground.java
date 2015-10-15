package playground;

import java.io.File;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;

import de.uni_koblenz.west.cidre.common.utils.RDFFileIterator;

public class Playground {

	public static void main(String[] args) {
		for (Statement statement : new RDFFileIterator(
				new File("/home/danijank/Downloads/testdata"), null)) {
			Resource subject = statement.getSubject();
			Property predicate = statement.getPredicate();
			RDFNode object = statement.getObject();
			System.out.println(subject + " " + predicate + " " + object);
		}
	}

}
