package mil.nga.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.nio.file.Files;

public class Test {

	public static void main (String[] args) {
		String path = "file:///mnt/nonstd/data/aero/vvod/LIB-UNCLASSIFIED-UNZIPPED/2017_07_20/vacag/lineage.doc";
		try {
			URI uri = new URI(path);
			Path p = Paths.get(uri);
			if (Files.exists(p)) {
				System.out.println("file => [ " + path + " ] EXISTS!");
			}
			else {
				System.out.println("File does not exist!");
			}
		}
		catch (URISyntaxException use) {
			use.printStackTrace();
		}
		
	}
}
