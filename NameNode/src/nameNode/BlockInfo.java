package nameNode;

import java.util.ArrayList;
import java.util.List;

public class BlockInfo {
	private int blockNO;
	private String fileName;
	private boolean status;
	private List<Integer> dn = new ArrayList<>();
	
	public BlockInfo() {
		//Assigning Default value
		status = true;
	}
	
	public int getBlockNO() {
		return blockNO;
	}
	public void setBlockNO(int blockNO) {
		this.blockNO = blockNO;
	}
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public boolean getStatus() {
		return status;
	}
	public void setStatus(boolean status) {
		this.status = status;
	}
	public List<Integer> getDn() {
		return dn;
	}
	public void setDn(List<Integer> dn) {
		this.dn = dn;
	}
	
	public String toString() {
		StringBuffer sf = new StringBuffer();
		sf.append("\n{ BlockNo : " + blockNO);
		sf.append(", Filename : " + fileName);
		sf.append(", BlockStatus : " + status);
		sf.append(", DNList : " + dn.toString() + "}");
		return sf.toString();
	}
}
