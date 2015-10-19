package playground;

import java.io.File;

import org.apache.jena.graph.Node;

import de.uni_koblenz.west.cidre.common.utils.RDFFileIterator;

public class Playground {

	public static void main(String[] args) {
		for (Node[] nodes : new RDFFileIterator(
				new File("/home/danijank/Downloads/testdata"), null)) {
			String delim = "";
			for (Node node : nodes) {
				System.out.print(delim);
				System.out.print(node.toString(true));
				delim = " ";
			}
			System.out.println();
		}
	}

}
