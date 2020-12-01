package nameNode;

public class FileInfo {
	
	private String fileName;
	private int startBlock;
	private int endBlock;
	private boolean status;
	private boolean isInUse;
	
	public FileInfo() {
		//Setting default values
		status = false;
	}
	
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public int getStartBlock() {
		return startBlock;
	}
	public void setStartBlock(int startBlock) {
		this.startBlock = startBlock;
	}
	public int getEndBlock() {
		return endBlock;
	}
	public void setEndBlock(int endBlock) {
		this.endBlock = endBlock;
	}
	public boolean getStatus() {
		return status;
	}
	public void setStatus(boolean status) {
		this.status = status;
	}
	public boolean isInUse() {
		return isInUse;
	}
	public void setInUse(boolean isInUse) {
		this.isInUse = isInUse;
	}
	
	public String toString() {
		StringBuffer sf = new StringBuffer();
		sf.append("\n{ FileName : " + fileName);
		sf.append(", startBlock : " + startBlock);
		sf.append(", endBlock : " + endBlock);
		sf.append(", isInUse : " + isInUse);
		sf.append(", status : " + status + "}");
		return sf.toString();
	}
}
