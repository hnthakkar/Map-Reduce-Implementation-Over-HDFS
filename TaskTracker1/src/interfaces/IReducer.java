package interfaces;

import java.io.File;
import java.util.List;

public interface IReducer {
	public boolean reduce(List<File> files, String outFile);
}
