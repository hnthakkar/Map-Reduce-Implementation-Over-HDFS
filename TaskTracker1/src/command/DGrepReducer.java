package command;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.List;

import interfaces.IReducer;

public class DGrepReducer implements IReducer{

	@Override
	public boolean reduce(List<File> files, String outFile) {
		//Just append in this case
		PrintWriter out = null;
		StringBuffer sb = new StringBuffer();
	    try {
			out = new PrintWriter(new FileOutputStream(outFile, false));
			
			for (File f : files) {
				sb.append(Files.readAllBytes(f.toPath()));
			}
			out.write(sb.toString());
			out.flush();
			return true;
		} catch (Exception e) {
			System.out.println("Error while reducing!!");
			return false;
		} finally {
			out.close();
		}
	}
}
