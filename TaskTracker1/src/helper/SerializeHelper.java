package helper;

import com.google.protobuf.ByteString;

import proto.Hdfs.AssignBlockRequest;
import proto.Hdfs.BlockLocationRequest;
import proto.Hdfs.CloseFileRequest;
import proto.Hdfs.ListFilesRequest;
import proto.Hdfs.OpenFileRequest;
import proto.Hdfs.ReadBlockRequest;
import proto.Hdfs.WriteBlockRequest;
import proto.MapReduce.JobStatusRequest;
import proto.MapReduce.JobSubmitRequest;

public class SerializeHelper {
	
	public static byte[] buildOpenFileReq(String fileName, boolean forRead) {
		OpenFileRequest.Builder req = OpenFileRequest.newBuilder();
		req.setFileName(fileName);
		req.setForRead(forRead);
		return req.build().toByteArray();
	}
	
	public static byte[] buildAssignBlockReq(String fileName, int noOfBlocks) {
		AssignBlockRequest.Builder req = AssignBlockRequest.newBuilder();
		req.setFileName(fileName);
		req.setNoOfBlocks(noOfBlocks);
		return req.build().toByteArray();
	}
	
	public static byte[] buildBlockLocationRequest(int blockNo) {
		BlockLocationRequest.Builder req = BlockLocationRequest.newBuilder();
		req.setBlockNo(blockNo);
		return req.build().toByteArray();
	}
	
	public static byte[] buildCloseFileRequest(String fileName) {
		CloseFileRequest.Builder req = CloseFileRequest.newBuilder();
		req.setFileName(fileName);
		return req.build().toByteArray();
	}
	
	public static byte[] buildListFileReq(String dir) {
		ListFilesRequest.Builder req = ListFilesRequest.newBuilder();
		req.setDirName(dir);
		return req.build().toByteArray();
	}
	
	public static byte[] buildWriteBlockReq(int blockNo, byte[] data) {
		WriteBlockRequest.Builder req = WriteBlockRequest.newBuilder();
		req.setBlockNo(blockNo);
		req.addData(ByteString.copyFrom(data));
		return req.build().toByteArray();
	}
	
	public static byte[] buildReadReq(int blockNo) {
		ReadBlockRequest.Builder req = ReadBlockRequest.newBuilder();
		req.setBlockNo(blockNo);
		return req.build().toByteArray();
	}
	
	/*private boolean isBlank(String input) {
		if (input == null || input == "")
			return true;
		return false;
	}*/
	
	public static byte[] buildJobSubmitRequest(String mapName, String reducerName, String inpFile, String outputFile, int noOfReducer) {
		JobSubmitRequest.Builder req = JobSubmitRequest.newBuilder();
		req.setMapName(mapName);
		req.setReducerName(reducerName);
		req.setInputFile(inpFile);
		req.setOutputFile(outputFile);
		req.setNumReduceTasks(noOfReducer);
		return req.build().toByteArray();
	}
	
	public static byte[] buildJobStatusRequest(int jobId) {
		JobStatusRequest.Builder req = JobStatusRequest.newBuilder();
		req.setJobId(jobId);
		return req.build().toByteArray();
	}
}
