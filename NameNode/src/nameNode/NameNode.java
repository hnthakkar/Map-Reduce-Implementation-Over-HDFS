package nameNode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintWriter;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.protobuf.InvalidProtocolBufferException;

import interfaces.INameNode;
import proto.Hdfs;
import proto.Hdfs.AssignBlockRequest;
import proto.Hdfs.AssignBlockResponse;
import proto.Hdfs.BlockLocationRequest;
import proto.Hdfs.BlockLocationResponse;
import proto.Hdfs.BlockReportRequest;
import proto.Hdfs.BlockReportResponse;
import proto.Hdfs.CloseFileRequest;
import proto.Hdfs.CloseFileResponse;
import proto.Hdfs.DNInfo;
import proto.Hdfs.ListFilesResponse;
import proto.Hdfs.OpenFileRequest;
import proto.Hdfs.OpenFileResponse;
import proto.MapReduce.FileInfoRequest;
import proto.MapReduce.FileInfoResponse;

public class NameNode implements INameNode{
	
	public static final int replicationFactor = 3;
	public static final int noOfTotalDN = 4;
	public static final String fileInfoStorage = ".//tmp//fileEntry";
	public static final String blockNoStorage = ".//tmp//diskBlockEntry";
	public static final String logStorage = "./log/NN.log";
	public static final int fileInUseRes = 2;
	public static final int error = 0;
	public static final int OK = 1;
	
	private Integer nextBlock = 1;
	private int diskBlockEntry = 15;
	private Map<Integer, BlockInfo> blockMap = new ConcurrentHashMap<>();
	private Map<Integer, DNInfo> dnInfo = new ConcurrentHashMap<>();
	private Map<String, FileInfo> fileMap = new ConcurrentHashMap<>();
	private PrintWriter logWriter = null;
	private boolean debugWriter = true;
	private PrintWriter fileWriter = null;
	private PrintWriter blockWriter = null;
	private boolean isInitialized = false;
	
	public NameNode() {
		try {
			initialize();
		} catch (Exception e) {
			System.out.println("Error initializing!!");
		}
	}
	
	private void initialize() throws Exception {
		logWriter = new PrintWriter(logStorage, "UTF-8");
		fileWriter = new PrintWriter(new FileOutputStream(fileInfoStorage, true));
		
		File f = new File(fileInfoStorage);
		if(f.exists() && !f.isDirectory()) { 
			try (BufferedReader br = new BufferedReader(new FileReader(f))) {
			    String line;
			    //filename:startBlock:endBlock
			    while ((line = br.readLine()) != null) {
			       String[] info = line.split(":");
			       if (info.length == 3) {
			    	   String fileName = info[0];
			    	   int startBlock = Integer.parseInt(info[1]);
			    	   int endBlock = Integer.parseInt(info[2]);
			    	   processFile(fileName, endBlock - startBlock + 1, false, startBlock, endBlock);
			    	   /*FileInfo fInfo = new FileInfo();
			    	   fInfo.setFileName(info[0]);
			    	   fInfo.setStartBlock(Integer.parseInt(info[1]));
			    	   fInfo.setEndBlock(Integer.parseInt(info[2]));
			    	   fileMap.put(info[0], fInfo);*/
			       }
			    }
			}
		}
		
		File f1 = new File(blockNoStorage);
		if(f1.exists() && !f1.isDirectory()) { 
			try (BufferedReader br = new BufferedReader(new FileReader(f1))) {
				String line;
				if ((line = br.readLine()) != null) {
					nextBlock = Integer.parseInt(line);
				} else {
					nextBlock = 1;
				}
			}
		}
		isInitialized = true;
		if (debugWriter) {
			/*logWriter.append("NameNode initaillization complete..");
			logWriter.flush();*/
			System.out.println("NameNode initaillization complete..");
		}
		blockWriter = new PrintWriter(new FileOutputStream(blockNoStorage, false));
		blockWriter.write((nextBlock + diskBlockEntry) + "");
		blockWriter.close();
	}
	
	private List<Hdfs.BlockInfo> processFile(String fileName, int noOfBlocks, boolean isNew, int v_start, int v_end) {
		if(fileMap.containsKey(fileName)) {
			return null;
		}
		int start = 0;
		int end = 0;
		if (isNew) {
			System.out.println("Lets see");
			int[] edges = assignBlockNumber(noOfBlocks);
			start = edges[0];
			end = edges[1];
		} else {
			start = v_start;
			end = v_end;
		}
		
		StringBuffer sb = null;
		if(debugWriter) {
			sb = new StringBuffer();
			sb.append("**************AssignBlock Response***************");
			sb.append("\nFor File : " + fileName + "Start Block : " + start + " End Block : " + end);
		}
		
		FileInfo fInfo = new FileInfo();
		fInfo.setFileName(fileName);
		fInfo.setStartBlock(start);
		fInfo.setEndBlock(end);
		List<Hdfs.BlockInfo> blockInfo = new ArrayList<>();
		for (int i = start; i <= end; i++) {
			Hdfs.BlockInfo.Builder block = Hdfs.BlockInfo.newBuilder();
			block.setBlockNo(i);
			//List<Integer> dns = Collections.EMPTY_LIST;
			//if (isNew) {
				List<Integer> dns = processBlock(fileName, i, isNew);
				block.addAllDNNumber(dns);
			//}
			if(debugWriter) {
				sb.append("\n For Block : " + i + " assigned DNS : " + dns.toString());
			}
			blockInfo.add(block.build());
		}
		fileMap.put(fileName, fInfo);
		if(isNew) {
			fileWriter.append(fileName + ":" + start + ":" + end + "\n");
			fileWriter.flush();
		}	
		if (debugWriter) {
			sb.append("***********************************************");
			/*synchronized (logWriter) {
				logWriter.append(sb.toString());
				logWriter.flush();
			}*/
			System.out.println(sb.toString());
		}
		return blockInfo;
	}
	
	
	
	private int[] assignBlockNumber(int noOfBlocks) {
		int[] blockEdges = new int[2];
		synchronized (nextBlock) {
			blockEdges[0] = nextBlock;
			blockEdges[1] = nextBlock + noOfBlocks -1;
			nextBlock += noOfBlocks;
		}
		if (Math.abs(nextBlock - diskBlockEntry) > 10) {
			try {
				blockWriter = new PrintWriter(new FileOutputStream(blockNoStorage, false));
				diskBlockEntry += 15; 
				blockWriter.write(diskBlockEntry + "");
				blockWriter.flush();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}finally {
				blockWriter.close();
			}
		}
		return blockEdges;
	}
	
	private List<Integer> assignDN() {
		final Random random = new Random();
	    final Set<Integer> DNSet = new HashSet<>();
	    while (DNSet.size() < replicationFactor) {
	    	DNSet.add(random.nextInt(noOfTotalDN) + 1);
	    }
	    System.out.println(DNSet.toString());
	    List<Integer> dnList = new ArrayList<>(DNSet);
	    return dnList;
	}
	
	private List<Integer> processBlock(String fileName, int blockNO, boolean isNew) {
		BlockInfo bInfo = new BlockInfo();
		bInfo.setBlockNO(blockNO);
		bInfo.setFileName(fileName);
		List<Integer> dnList = new ArrayList<>();
		if (isNew) {
			dnList = assignDN();
		}
		bInfo.setDn(dnList);
		blockMap.put(blockNO, bInfo);
		return dnList;
	}
	
	private boolean checkFileStatus(String fileName) {
		boolean ret = true;
		FileInfo fInfo = fileMap.get(fileName);
		int start = fInfo.getStartBlock();
		int end = fInfo.getEndBlock();
		BlockInfo bInfo = null;
		for (int i = start; i <= end; i++) {
			bInfo = blockMap.get(i);
			if (bInfo!= null && bInfo.getStatus())
				continue;
			ret = false;
			break;
		}
		return ret;
	}

	private byte[] fileInUseResponse() {
		OpenFileResponse.Builder res = OpenFileResponse.newBuilder();
		res.setStatus(fileInUseRes);
		return res.build().toByteArray();
	}
	
	private byte[] openFileErrorRes() {
		OpenFileResponse.Builder res = OpenFileResponse.newBuilder();
		res.setStatus(error);
		return res.build().toByteArray();
	}

	@Override
	public byte[] openFile(byte[] inp) throws RemoteException {
		while(!isInitialized) {
			System.out.println("waiting for initialization");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		try {
			OpenFileRequest req = OpenFileRequest.parseFrom(inp);
			String fileName = req.getFileName();
			FileInfo fileInfo = fileMap.get(fileName);
			
			if (!fileInfo.getStatus()) {
				return openFileErrorRes();
			}
			
			if (fileInfo.isInUse()) {
				return fileInUseResponse();
			}
			OpenFileResponse.Builder res = OpenFileResponse.newBuilder();
			
			dnInfo.forEach((k,v)->{
				DNInfo.Builder dnRes = DNInfo.newBuilder();
				dnRes.setDNNumber(k);
				dnRes.setIp(((DNInfo)v).getIp());
				dnRes.setPort(((DNInfo)v).getPort());
				res.addDnInfo(dnRes);
			});
			
			int startBlock = fileInfo.getStartBlock();
			int endBlock = fileInfo.getEndBlock();
			
			StringBuffer sb = null;
			if (debugWriter) {
				sb = new StringBuffer();
				sb.append("\n***********OpenFile Req**********");
				sb.append("\nFor File : " + fileName + " Start Block : " + startBlock + " endBlock " + endBlock);
			}
			
			for (int i = startBlock; i <= endBlock; i++) {
				BlockInfo bInfo = blockMap.get(i);
				List<Integer> dnList = bInfo.getDn();
				if (debugWriter) {
					sb.append("\n For Block " + i + " DNS : " + dnList.toString());
				}
				/*bInfo.getDn().forEach(itme -> {
					Hdfs.BlockInfo.Builder blockRes = Hdfs.BlockInfo.newBuilder();
					blockRes.setBlockNo(i);
					
				});*/
				Hdfs.BlockInfo.Builder protoBLockInfo = Hdfs.BlockInfo.newBuilder();
				Iterator<Integer> it = dnList.iterator();
				while(it.hasNext()) {
					int dnNumber = it.next();
					protoBLockInfo.addDNNumber(dnNumber);
					protoBLockInfo.setBlockNo(i);
				}
				res.addBlockInfo(protoBLockInfo);
			}
			if (debugWriter) {
				sb.append("\n*****************************************");
				/*synchronized (logWriter) {
					logWriter.append(sb.toString());
					logWriter.flush();
				}*/
				System.out.println(sb.toString());
			}
			
			return res.setStatus(OK).build().toByteArray();
		} catch (Exception e) {
			e.printStackTrace();
			return openFileErrorRes();
		}
	}


	@Override
	public byte[] closeFile(byte[] inp) throws RemoteException {
		while(!isInitialized) {
			System.out.println("waiting for initialization");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		try {
			CloseFileRequest req = CloseFileRequest.parseFrom(inp);
			String fileName = req.getFileName();
			if (debugWriter) {
				StringBuffer sb = new StringBuffer();
				sb.append("\n***********Close Request*************");
				sb.append("\nFile name : " + fileName);
				sb.append("\n*************************************");
				/*synchronized (logWriter) {
					logWriter.append(sb.toString());
					logWriter.flush();
				}*/
				System.out.println(sb.toString());
			}
			
			fileMap.get(fileName).setInUse(false);
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
			CloseFileResponse.Builder res = CloseFileResponse.newBuilder();
			res.setStatus(error);
			return res.build().toByteArray();
		}
		
		CloseFileResponse.Builder res = CloseFileResponse.newBuilder();
		res.setStatus(OK);
		return res.build().toByteArray();
	}


	@Override
	public byte[] getBlockLocations(byte[] inp) throws RemoteException {
		while(!isInitialized) {
			System.out.println("waiting for initialization");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		BlockLocationResponse.Builder res = BlockLocationResponse.newBuilder();;
		try {
			BlockLocationRequest req = BlockLocationRequest.parseFrom(inp);
			int blockNo = req.getBlockNo();
			StringBuffer sb = null;
			if (debugWriter) {
				sb = new StringBuffer();
				sb.append("\n************BlockLocation Req*********");
				sb.append("\nFor Block" + blockNo);
			}
			BlockInfo bInfo = blockMap.get(blockNo);
			Hdfs.BlockInfo.Builder protoBLockInfo = Hdfs.BlockInfo.newBuilder();
			List<Integer> dnList = bInfo.getDn();
			if (debugWriter) {
				sb.append("\nResponse : " + dnList.toString());
				sb.append("\n*************************************");
				/*synchronized (logWriter) {
					logWriter.append(sb.toString());
					logWriter.flush();
				}*/
				System.out.println(sb.toString());
			}
			
			dnInfo.forEach((k,v)->{
				DNInfo.Builder dnRes = DNInfo.newBuilder();
				dnRes.setDNNumber(k);
				dnRes.setIp(((DNInfo)v).getIp());
				dnRes.setPort(((DNInfo)v).getPort());
				res.addDnInfo(dnRes);
			});
			
			Iterator<Integer> it = dnList.iterator();
			while(it.hasNext()) {
				int dnNumber = it.next();
				protoBLockInfo.addDNNumber(dnNumber);
			}
			res.setBlockInfo(protoBLockInfo).setStatus(OK);
			return res.build().toByteArray();
		} catch (Exception e) {
			e.printStackTrace();
			res.setStatus(error);
			return res.build().toByteArray();
		}
	}


	@Override
	public byte[] assignBlock(byte[] inp) throws RemoteException {
		while(!isInitialized) {
			System.out.println("waiting for initialization");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		AssignBlockResponse.Builder res = AssignBlockResponse.newBuilder();
		
		try {
			AssignBlockRequest req = AssignBlockRequest.parseFrom(inp);
			String fileName = req.getFileName();
			int noOfBlocks = req.getNoOfBlocks();
			StringBuffer sb = new StringBuffer();
			if (debugWriter) {          
				sb.append("\n************AssignBlock Request*******************");
				sb.append("\nFilename: " + fileName + " NoOfBlocks : " + noOfBlocks);
				sb.append("\n**************************************************");
				/*synchronized (logWriter) {
					logWriter.append(sb.toString());
					logWriter.flush();
				}*/
				System.out.println(sb.toString());
			}	
			dnInfo.forEach((k,v)->{
				DNInfo.Builder dnRes = DNInfo.newBuilder();
				dnRes.setDNNumber(k);
				dnRes.setIp(((DNInfo)v).getIp());
				dnRes.setPort(((DNInfo)v).getPort());
				res.addDnInfo(dnRes);
			});
			res.addAllBlockInfo(processFile(fileName, noOfBlocks, true, 0, 0));
			res.setStatus(OK);
			return res.build().toByteArray();
		} catch (Exception e) {
			e.printStackTrace();
			res.setStatus(error);
			return res.build().toByteArray();	
		}
	}

	@Override
	public byte[] list(byte[] inp) throws RemoteException {
		while(!isInitialized) {
			System.out.println("waiting for initialization");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		//return all files from FileMap
		try {
			ListFilesResponse.Builder res = ListFilesResponse.newBuilder();
			StringBuffer sb = new StringBuffer();
			sb.append("\n************List File Response********");
			fileMap.forEach((k,v)->{
				if (v.getStatus()) {
					res.addFileNames(k);
					sb.append("\n" + k);
				}	
			});
			sb.append("\n*************************************");
			synchronized (logWriter) {
				logWriter.append(sb.toString());
				logWriter.flush();
			}
			return res.setStatus(OK).build().toByteArray();
		} catch (Exception e) {
			ListFilesResponse.Builder res = ListFilesResponse.newBuilder();
			return res.setStatus(error).build().toByteArray();
		}
	}
	
	@Override
	public byte[] getFileInfo(byte[] inp ) throws RemoteException {
		while(!isInitialized) {
			System.out.println("waiting for initialization");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		FileInfoResponse.Builder res = FileInfoResponse.newBuilder();
		try {
			FileInfoRequest req = FileInfoRequest.parseFrom(inp);
			String fileName = req.getFileName();
			FileInfo fInfo = fileMap.get(fileName);
			res.setStartBlock(fInfo.getStartBlock());
			res.setEndBlock(fInfo.getEndBlock());
			return res.setStatus(OK).build().toByteArray();
		} catch(Exception e) {
			System.out.println("Exception reading fileInfo");
			res.setStatus(error);
			return res.build().toByteArray();
		}
	}
	
	@Override
	public byte[] blockReport(byte[] inp) throws RemoteException {
		while(!isInitialized) {
			System.out.println("waiting for initialization");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		BlockReportResponse.Builder res = BlockReportResponse.newBuilder();
		try {
			BlockReportRequest req = BlockReportRequest.parseFrom(inp);
			int dataNodeNo = req.getId();
			DNInfo dInfo = req.getLocation();
			if (!dnInfo.containsKey(dataNodeNo)) {
				dnInfo.put(dataNodeNo, dInfo);
			}
			
			List<Integer> blockList = req.getBlockNoList();
			if (debugWriter) {
				//writeBlockReqToLog(dataNodeNo, dInfo, blockList);
			}
			for (Integer block : blockList) {
				if (blockMap.containsKey(block)) {
					BlockInfo bInfo = blockMap.get(block);
					bInfo.setStatus(true);
					if (!bInfo.getDn().contains(dataNodeNo)) {
						System.out.println("Block reported by other DN, May be NN restarted!!");
						/*if (bInfo.getDn().size() == 0) {
							bInfo.getDn().add(new ArrayList<Integer>().add(dataNodeNo));
						}*/
						bInfo.getDn().add(dataNodeNo);
						//blockMap.put(block, bInfo);
					}
					if (checkFileStatus(bInfo.getFileName())) {
						fileMap.get(bInfo.getFileName()).setStatus(true);
					}
				} else {
					System.out.println("Non-existent block reported!!!");
				}
			}
			res.setStatus(OK);
			return res.build().toByteArray();
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
			res.setStatus(error);
			return res.build().toByteArray();
		}
	}

	private void writeBlockReqToLog(int dataNode, DNInfo dInfo, List<Integer> blockList) {
		StringBuffer sb = new StringBuffer();
		sb.append("\n*************Recevied Block report****************");
		sb.append("\nBlockReport Received from : " + dataNode);
		sb.append("\n have IP : " + dInfo.getIp());
		sb.append("\n port : " + dInfo.getPort());
		sb.append("\nBlocks reported : " + blockList.toString());
		sb.append("\n*****************************************************");
		/*synchronized (logWriter) {
			logWriter.append(sb.toString());
			logWriter.flush();
		}*/
		System.out.println("\n" + sb.toString());
	}
	/*@Override
	public byte[] heartBeat(byte[] inp) throws RemoteException {
		return null;
	}*/
}
