package command;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.regex.Pattern;

import interfaces.IMapper;

public class DGrepMapper implements IMapper {

	String pattern = "ClientAbortException:  java.net.SocketException: Software caused connection abort: socket write error";

	@Override
	public boolean map(File f, String outFile) {
		StringBuffer sb = new StringBuffer();
		PrintWriter out = null;
		
		try (BufferedReader br = new BufferedReader(new FileReader(f))) {
			out = new PrintWriter(new FileOutputStream(new File("./output/" + outFile), false));
		    String line;
		    while ((line = br.readLine()) != null) {
		    	if (line.indexOf(pattern) > -1){
					sb.append("\n" + line);
				}
		    }
		    
		    out.write(sb.toString());
			out.flush();
		} catch (Exception e) {
			System.out.println("Problem reading the file");
			return false;
		}finally {
			out.close();
		}
		return true;
	}
}
