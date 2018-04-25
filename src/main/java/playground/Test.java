package playground;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

public class Test {

	public static void main(String[] args) throws IOException {
		File master = new File("/tmp/master");
		File newDir = new File("/tmp/test");
		FileUtils.moveDirectory(master, newDir);
	}

}
