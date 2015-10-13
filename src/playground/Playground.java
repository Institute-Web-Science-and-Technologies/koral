package playground;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class Playground {

	public static void main(String[] args) {
		try (FileInputStream fis = new FileInputStream(
				"/home/danijank/Downloads/testdata/exampleGraph2.n3");
		// BufferedInputStream bis = new BufferedInputStream(fis);
		) {
			long skippedBytes = fis.skip(10);
			System.out.println("Skipped " + skippedBytes + "/10 bytes.");
			skippedBytes = fis.skip(-5);
			System.out.println("Skipped " + skippedBytes + "/-5 bytes.");
			byte[] content = new byte[10];
			fis.read(content);
			System.out.println(new String(content));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
