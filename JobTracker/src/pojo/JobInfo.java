package pojo;

import java.util.ArrayList;
import java.util.List;

public class JobInfo {

	private int jobID = 0;
	private String inpFileName = null;
	private String mapFile = null;
	private String reduceFile = null;
	private String outputFileName = null;
	private int noOfReducer = 0;
	private boolean jobStatus = false;
	private List<Integer> blocks = new ArrayList<>();
	private FileInfo fileInfo = null;
	private boolean mapTaskDone = false;
	
	public boolean isMapTaskDone() {
		return mapTaskDone;
	}
	public void setMapTaskDone(boolean mapTaskDone) {
		this.mapTaskDone = mapTaskDone;
	}
	public int getJobID() {
		return jobID;
	}
	public void setJobID(int jobID) {
		this.jobID = jobID;
	}
	public String getInpFileName() {
		return inpFileName;
	}
	public void setInpFileName(String inpFileName) {
		this.inpFileName = inpFileName;
	}
	public String getMapFile() {
		return mapFile;
	}
	public void setMapFile(String mapFile) {
		this.mapFile = mapFile;
	}
	public String getReduceFile() {
		return reduceFile;
	}
	public void setReduceFile(String reduceFile) {
		this.reduceFile = reduceFile;
	}
	public String getOutputFileName() {
		return outputFileName;
	}
	public void setOutputFileName(String outputFileName) {
		this.outputFileName = outputFileName;
	}
	public int getNoOfReducer() {
		return noOfReducer;
	}
	public void setNoOfReducer(int noOfReducer) {
		this.noOfReducer = noOfReducer;
	}
	public boolean isJobStatus() {
		return jobStatus;
	}
	public void setJobStatus(boolean jobStatus) {
		this.jobStatus = jobStatus;
	}
	public List<Integer> getBlocks() {
		return blocks;
	}
	public void addToBlocks(int block) {
		blocks.add(block);
	}
	public FileInfo getFileInfo() {
		return fileInfo;
	}
	public void setFileInfo(FileInfo fileInfo) {
		this.fileInfo = fileInfo;
	}
}
