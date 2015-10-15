package playground;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.RDFDataMgr;

public class Playground {

	public static void main(String[] args) {
		Model model = RDFDataMgr
				.loadModel("/home/danijank/Downloads/exampleGraph.n3");
		System.out.println(model);
		StmtIterator it = model.listStatements();
		while (it.hasNext()) {
			Statement statement = it.next();
			Resource subject = statement.getSubject();
			System.out.println("subject: " + subject);
			Property predicate = statement.getPredicate();
			System.out.println("property: " + predicate);
			RDFNode object = statement.getObject();
			System.out.println("object: " + object);
		}
	}

}
