import java.io.FileInputStream;
import java.io.InputStream;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import interfaces.IJobTracker;
import interfaces.INameNode;
import pojo.FileInfo;
import pojo.JobInfo;
import proto.MapReduce.FileInfoRequest;
import proto.MapReduce.FileInfoResponse;
import proto.MapReduce.HeartBeatRequest;
import proto.MapReduce.HeartBeatResponse;
import proto.MapReduce.JobStatusRequest;
import proto.MapReduce.JobStatusResponse;
import proto.MapReduce.JobSubmitRequest;
import proto.MapReduce.JobSubmitResponse;
import proto.MapReduce.MapTaskInfo;
import proto.MapReduce.MapTaskStatus;

public class JobTracker implements IJobTracker{
	
	private static String NN_IP = "";
	private static int NN_PORT = 0;
	private static int PORT = 0;
	
	public static final int submitted = 1;
	public static final int completed = 1;
	
	private INameNode nameNode = null;
	public static final int error = 0;
	public static final int OK = 1;
	public ConcurrentHashMap<Integer, JobInfo> jobMap = new ConcurrentHashMap<>();
	public ConcurrentHashMap<Integer, Integer> blockStatus = new ConcurrentHashMap<>();
	public ConcurrentHashMap<Integer, ArrayList<Boolean>> reducerStatus = new ConcurrentHashMap<>();
	private List<String> queue = null;
	private Registry registry;
	
	private Integer nextJobNumber = 1;
	
	static {
		try {
			Properties prop = new Properties();
			InputStream  input = new FileInputStream("./JTconfig.properties");
			prop.load(input);
			
			PORT = Integer.parseInt(prop.getProperty("port"));
			NN_IP = prop.getProperty("NNIP");
			NN_PORT = Integer.parseInt(prop.getProperty("NNPORT"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		JobTracker jt = new JobTracker();
		jt.startRegistry();
		jt.register();
		jt.process();
	}

	private void startRegistry() {
		try {
			//registry = LocateRegistry.getRegistry(PORT);
			//if (registry == null) {
				registry = LocateRegistry.createRegistry(PORT);
			//}	
		} catch (RemoteException e) {
			System.out.println("Error Starting registry!!");
		}
	}
	
	private void register() {
		try {
			IJobTracker stub = (IJobTracker) UnicastRemoteObject.exportObject(this, 0);
			registry.rebind("JT", stub);
		}catch(Exception e) {
			System.out.println("Error while registering!!");
		}
	}
	
	private void process() {
		getNameNodeStub();
		queue = new LinkedList<>();
	}
	
	private int getNextJobId() {
		int ret = 0;
		synchronized (nextJobNumber) {
			ret = nextJobNumber;
			nextJobNumber++;
		}
		return ret;
	}
	
	@Override
	public byte[] jobSubmit(byte[] inp) throws RemoteException{
		String inpFileName = null;
		String mapFile = null;
		String reduceFile = null;
		String outputFileName = null;
		int noOfReducer = 0;
		JobSubmitResponse.Builder res = JobSubmitResponse.newBuilder();;
		try {
			JobSubmitRequest req = JobSubmitRequest.parseFrom(inp);
			inpFileName = req.getInputFile();
			mapFile = req.getMapName();
			reduceFile = req.getReducerName();
			outputFileName = req.getOutputFile();
			noOfReducer = req.getNumReduceTasks();
			int jobID = getNextJobId();
			JobInfo jInfo = new JobInfo();
			jInfo.setJobID(jobID);
			jInfo.setInpFileName(inpFileName);
			jInfo.setMapFile(mapFile);
			jInfo.setReduceFile(reduceFile);
			jInfo.setNoOfReducer(noOfReducer);
			jInfo.setOutputFileName(outputFileName);
			
			FileInfoRequest.Builder nnreq = FileInfoRequest.newBuilder();
			nnreq.setFileName(inpFileName);
			byte[] resBytes = nameNode.getFileInfo(nnreq.build().toByteArray());
			if (resBytes == null) {
				throw new Exception();
			}
			FileInfoResponse nnres = FileInfoResponse.parseFrom(resBytes);
			if (nnres.getStatus() == error) {
				throw new Exception();
			}
			FileInfo fInfo = new FileInfo();
			fInfo.setStartBlock(nnres.getStartBlock());
			fInfo.setEndBlock(nnres.getEndBlock());
			
			jInfo.setFileInfo(fInfo);    
			jobMap.put(jobID, jInfo);
			
			// Create one more queue, producer-consumer, add JobID
			
			int startBlock = fInfo.getStartBlock();
			int endBlock = fInfo.getEndBlock();
			
			for (int i = startBlock; i <= endBlock; i++) {
				queue.add("" + jobID + "_" + i);
			}
			res.setStatus(OK);
			res.setJobId(jobID);
			return res.build().toByteArray();
		} catch (Exception e) {
			System.out.println("Error while reading job Request");
			getNameNodeStub();
			res.setStatus(error);
			return res.build().toByteArray();
		}
	}

	@Override
	public byte[] getJobStatus(byte[] inp) throws RemoteException {
		JobStatusResponse.Builder res = JobStatusResponse.newBuilder();
		try {
			JobStatusRequest req = JobStatusRequest.parseFrom(inp);
			int jobID = req.getJobId();
			JobInfo jobInfo = jobMap.get(jobID);
			
			if (jobInfo.isJobStatus()) {
				res.setStatus(OK);
				res.setJobDone(true);
				return res.build().toByteArray();
			}
			
			FileInfo fInfo = jobInfo.getFileInfo();
			int startBlock = fInfo.getStartBlock();
			int endBlock = fInfo.getEndBlock();
			
			int totalBlock = endBlock - startBlock + 1;
			res.setJobDone(false);
			res.setTotalMapTasks(totalBlock);
			
			int totalMapStarted = 0;
			for (int i = startBlock; i <= endBlock; i++) {
				if (blockStatus.containsKey(i) && blockStatus.get(i) > 0) {
					totalMapStarted++;
				}
			}
			res.setNumMapTasksStarted(totalMapStarted);
			res.setTotalReduceTasks(jobInfo.getNoOfReducer());
			
			int totalReduceStarted = 0;
			
			ArrayList<Boolean> status = null;
			if (reducerStatus.containsKey(jobID)) {
				status = reducerStatus.get(jobID);
			}
			
			if (status == null) {
				res.setNumReduceTasksStarted(0);
			} else {
				for (int i = 0; i < status.size(); i++) {
					if (status.get(i)) {
						totalReduceStarted++;
					}
				}
				res.setNumReduceTasksStarted(totalReduceStarted);
			}
			
			return res.setStatus(OK).build().toByteArray();
			
		} catch (Exception e) {
			res.setStatus(error);
			return res.build().toByteArray();
		}
	}

	@Override
	public byte[] heartBeat(byte[] inp) throws RemoteException {
		HeartBeatResponse.Builder res = HeartBeatResponse.newBuilder();
		try{
			HeartBeatRequest req = HeartBeatRequest.parseFrom(inp);
			int TT = req.getTaskTrackerId();
			int freeMapSlots = req.getNumMapSlotsFree();
			//int freeReduceSlots = req.getNumReduceSlotsFree();
			List<MapTaskStatus> mapTaskList = req.getMapStatusList();
			List<Integer> jobIds = new ArrayList<>();
			
			for (int i = 0; i < mapTaskList.size(); i++) {
				MapTaskStatus status = mapTaskList.get(i);
				int jobID = status.getJobId();
				jobIds.add(jobID);
				//SAME as BLOCKID;
				int blockID = status.getTaskId();
				if (status.getTaskCompleted()) {
					blockStatus.put(blockID, completed);
					//queue.remove("" + jobID + "_" +blockID);
				}
			}
			checkAllJobStatus(jobIds);
			
			List<MapTaskInfo> mapTasks = new ArrayList<>();
			//Iterator<String> it = queue.iterator();
			while (freeMapSlots > 0) {
				String job_task = null;
				synchronized (queue) {
					if (queue.size() > 0) {
						job_task = queue.get(0);
						queue.remove(0);
					} else {
						break;
					}
				}
				freeMapSlots--;
				if (job_task != null)
					System.out.println("Task found : " + job_task);
				if (job_task == null) {
					break;
				}
				String arg[] = job_task.split("_");
				int jobID = Integer.parseInt(arg[0]);
				int taskID = Integer.parseInt(arg[1]);
				blockStatus.put(taskID, submitted);
				MapTaskInfo.Builder task = MapTaskInfo.newBuilder();
				task.setJobId(jobID);
				task.setTaskId(taskID);
				task.setMapName(jobMap.get(jobID).getMapFile());
				mapTasks.add(task.build());
			}
			res.addAllMapTasks(mapTasks);
			res.setStatus(OK);
			return res.build().toByteArray();
		} catch (Exception e) {
			res.setStatus(error);
			return res.build().toByteArray();
		}
	}
	
	private void checkAllJobStatus(List<Integer> jobIds) {
		for (int i = 0; i < jobIds.size(); i++) {
			int jobID = jobIds.get(i);
			JobInfo jInfo = jobMap.get(jobID);
			FileInfo fInfo = jInfo.getFileInfo();
			int startBlock = fInfo.getStartBlock();
			int endBlock = fInfo.getEndBlock();
			boolean mapStatus = true;
			for (i = startBlock; i <= endBlock; i++ ) {
				if (blockStatus.get(i) == completed) {
					continue;
				}
				mapStatus = false;
				break;
			}
			
			if (mapStatus) {
				jInfo.setMapTaskDone(true);
				//TODO start the reduce task
			}
		}
	}
	
	public void getNameNodeStub() {
		try {
			nameNode = (INameNode)Naming.lookup("rmi://" + NN_IP + ":" + NN_PORT + "/" + "NN");
		} catch (Exception e) {
			System.out.println("JobTracker : Error connecting to NN");
		}
	}
}
