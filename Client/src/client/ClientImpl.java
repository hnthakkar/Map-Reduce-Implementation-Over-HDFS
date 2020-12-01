package client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import helper.SerializeImpl;
import interfaces.IClient;
import interfaces.IDataNode;
import interfaces.INameNode;
import proto.Hdfs.AssignBlockRequest;
import proto.Hdfs.AssignBlockResponse;
import proto.Hdfs.BlockInfo;
import proto.Hdfs.DNInfo;
import proto.Hdfs.ListFilesResponse;
import proto.Hdfs.OpenFileResponse;
import proto.Hdfs.ReadBlockResponse;
import proto.Hdfs.WriteBlockRequest;
import proto.Hdfs.WriteBlockResponse;

public class ClientImpl implements IClient{
	
	public static final int BLOCK_SIZE_MB = 32;
	public static final int BLOCK_SIZE = 1024 * 1024 * BLOCK_SIZE_MB;
	public static final int error = 0;
	public static final int OK = 1;
	public static final String logStorage = "./log/Client.log";
	public static final String OUTPUTDIR = "./output/";
	
	private static String NN_IP = "";
	private static int NN_PORT = 0;
	
	private PrintWriter logWriter = null;
	private boolean debugWriter = true;
	private INameNode nameNodeStub;
	private Map<Integer, IDataNode> dnStubs = new ConcurrentHashMap<>();
		
	public ClientImpl(String NNIP, int NNPORT) {
		NN_IP = NNIP;
		NN_PORT = NNPORT;
		initailize();
	}
	
	private void initailize() {
		try {
			logWriter = new PrintWriter(logStorage, "UTF-8");
		} catch (Exception e) {
			e.printStackTrace();
		}	
		getNNStub();
	}
	
	public boolean get(String fileName, boolean forRead) throws RemoteException{
		StringBuffer sb = null;
		if (debugWriter) {
			sb = new StringBuffer();
			sb.append("\n***********GET ***********");
			sb.append("\nGet Req for File : " + fileName + " with Read : " + forRead);
		}
		OpenFileResponse res = null;
		try {
			byte[] resBytes = nameNodeStub.openFile(SerializeImpl.buildOpenFileReq(fileName, forRead));
			res = OpenFileResponse.parseFrom(resBytes);
		} catch (Exception e) {
			getNNStub();
			e.printStackTrace();
			return false;
		}
		if(res.getStatus() != 1) {
			return sendCloseReq(fileName, false);
		}	
		List<DNInfo> dnList = res.getDnInfoList();
		
		Iterator<DNInfo> dnIt = dnList.iterator();
		while (dnIt.hasNext()) {
			DNInfo dn = dnIt.next();
			if (debugWriter) {
				sb.append("\nRes DN : " + dn.getDNNumber() + " :" +  dn.getIp() + " :" + dn.getPort());
			}
			getDataNodeStub(dn.getDNNumber(), dn.getIp(), dn.getPort());
		}
		
		List<BlockInfo> blockList = res.getBlockInfoList();
		Iterator<BlockInfo> it = blockList.iterator();
		while (it.hasNext()) {
			BlockInfo blockInfo = it.next();
			int blockNo = blockInfo.getBlockNo();
			List<Integer> dnNoList = blockInfo.getDNNumberList();
			if (debugWriter) {
				sb.append("\n Block No : " + blockNo + " DNList : " + dnNoList.toString());
			}
			Iterator<Integer> blockDNIt = dnNoList.iterator();
			byte[] block = null;
			while (blockDNIt.hasNext()) {
				int blockDNNo = blockDNIt.next();
				block = readBlock(blockDNNo, blockNo);
				if (block != null) {
					if (debugWriter) {
						sb.append("\nRead : " + blockNo);
					}	
					break;
				}	
			}
			
			if (block == null){
				System.out.println("Problem reading the block : " + blockNo);
				return sendCloseReq(fileName, false);
			}
			appendToFile(fileName, block);
			if (debugWriter) {
				sb.append("\nBlock : " + blockNo + " appended to OutputFile");
				System.out.println(sb.toString());
				/*synchronized (logWriter) {
					logWriter.append(sb.toString());
					logWriter.flush();
				}*/
			}
		}
		return sendCloseReq(fileName, true);
	}
	
	private boolean sendCloseReq(String fileName, boolean trStatus) {
		try {
			//Do Nothing as of now with resBytes
			/*byte[] resBytes =*/ nameNodeStub.closeFile(SerializeImpl.buildCloseFileRequest(fileName));
			if(debugWriter) {
				/*synchronized (logWriter) {
					logWriter.append("*******Sending Close req for : " + fileName + "************");
				}*/
				System.out.println("*******Sending Close req for : " + fileName + "************");
			}
		} catch (RemoteException e) {
			getNNStub();
			e.printStackTrace();
		}
		return trStatus;
	}
	
	private void appendToFile(String outputFile, byte[] data) {
		try (
				FileWriter fw = new FileWriter(OUTPUTDIR + outputFile, true);
				BufferedWriter bw = new BufferedWriter(fw);
				PrintWriter out = new PrintWriter(bw)
			) {
				CharSequence charSeq = new String(data, "UTF-8");
				out.append(charSeq);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private byte[] readBlock(int dataNodeNo, int blockNo) {
		ByteString ret = null;
		IDataNode stub = null;
		try {
			stub = dnStubs.get(dataNodeNo);
			byte[] resBytes = stub.readBlock(SerializeImpl.buildReadReq(blockNo));
			ReadBlockResponse res = ReadBlockResponse.parseFrom(resBytes);
			if (res.getStatus() != 1) {
				return null;
			}
			ret = ByteString.copyFrom(res.getDataList());
		} catch (Exception e) {
			if (stub != null) {
				dnStubs.remove(dataNodeNo);
			}	
			//e.printStackTrace();
			System.out.println("DN : " + dataNodeNo + " unreachable!!");
			return null;
		} 
		return ret.toByteArray();
	}
	
	
	public List<String> list(String dirName) throws RemoteException{
		List<String> ret = null;
		try {
			byte[] resBytes = nameNodeStub.list(SerializeImpl.buildListFileReq(dirName));
			ListFilesResponse res = ListFilesResponse.parseFrom(resBytes);
			if(res.getStatus() != 1) {
				return ret;
			}
			ret = res.getFileNamesList();
			if (debugWriter) {
				/*synchronized (logWriter) {
					logWriter.append("***** List req : " + ret.toString() + "********");
				}*/
				System.out.println("***** List req : " + ret.toString() + "********");
			}
		} catch (Exception e) {
			getNNStub();
			//e.printStackTrace();
			System.out.println("ClientImpl Error : list");
		}
		return ret;
	}
	
	public boolean put(String path, String fileName) throws RemoteException {
		byte[] resBytes = null;
		StringBuffer sb = null;
		try {
			File file = new File(path, fileName);
			int numberOfBlocks = 0;
			if (file.exists()) {
				//Long fileSize = file.length();
				
				/*if (fileSize < BLOCK_SIZE) {
					numberOfBlocks = 1;
				} else {*/
					//Double d = fileSize/BLOCK_SIZE;
					//numberOfBlocks = ((Double)Math.ceil(file.length()/(BLOCK_SIZE))).intValue();
					System.out.println("File exists calling split!!");
					numberOfBlocks = splitFile(file, 1);
				//}	
			} else {
				System.out.println("File does not exists!!!!");
				return false;			
			}
			AssignBlockRequest.Builder req = AssignBlockRequest.newBuilder();
			req.setFileName(fileName);
			req.setNoOfBlocks(numberOfBlocks);
			resBytes = nameNodeStub.assignBlock(req.build().toByteArray());
			if (debugWriter) {
				sb = new StringBuffer();
				sb.append("\n**********PUT*************");
				sb.append("\nFile : " + (path + fileName));
			}
		} catch(Exception e) {
			e.printStackTrace();
			getNNStub();
			System.out.println("Error while assignning blocks");
			return false;
		}
		
		if (resBytes == null)
			return false;
		
		try {
			AssignBlockResponse res = AssignBlockResponse.parseFrom(resBytes);
			if (res.getStatus() == error) {
				return false;
			}
			List<DNInfo> dnList = res.getDnInfoList();
			Iterator<DNInfo> dnIt = dnList.iterator();
			while (dnIt.hasNext()) {
				DNInfo dn = dnIt.next();
				getDataNodeStub(dn.getDNNumber(), dn.getIp(), dn.getPort());
			}
			
			List<BlockInfo> blockList = res.getBlockInfoList();
			Iterator<BlockInfo> it = blockList.iterator();
			int splitBlockIndex = 1;
			while (it.hasNext()) {
				BlockInfo blockInfo = it.next();
				int blockNo = blockInfo.getBlockNo();
				List<Integer> dnNoList = blockInfo.getDNNumberList();
				Iterator<Integer> blockDNIt = dnNoList.iterator();
				
				byte[] block = readSpiltBlock(new File("./tmp", fileName + "_" + splitBlockIndex++));
				if (debugWriter) {
					sb.append("\n Read the Split File : " + "./tmp/" + fileName + "_" + splitBlockIndex);
				}
				while (blockDNIt.hasNext()) {
					int blockDNNo = blockDNIt.next();
					boolean writeSuc = writeToDataNode(blockDNNo, blockNo, block);
					if (debugWriter) {
						sb.append("Write Block : " + blockNo + " To DN : " + blockDNNo + " : " + writeSuc);
					}
					/*if (!writeSuc)
						System.out.println("Write failed, can be handled differently ");*/
				}
				System.out.println(sb.toString());
			}
		}catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error while writing to DN");
			return false;
		}
		return true;
	}
	
	private byte[] readSpiltBlock(File f) {
		try {
			return Files.readAllBytes(f.toPath());
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	private boolean writeToDataNode(int dataNode, int blockNo, byte[] data) {
		IDataNode DNNodeStub = dnStubs.get(dataNode);
		if (DNNodeStub == null) {
			System.out.println("DN : " + dataNode + " unreachable!!!");
			return false;
		}
		WriteBlockRequest.Builder writeReq = WriteBlockRequest.newBuilder();
		writeReq.setBlockNo(blockNo);
		writeReq.addData(ByteString.copyFrom(data));
		WriteBlockResponse writeRes = null;
		try {
			byte[] writeResBytes = DNNodeStub.writeBlock(writeReq.build().toByteArray());
			writeRes = WriteBlockResponse.parseFrom(writeResBytes);
		} catch (RemoteException e) {
			dnStubs.remove(dataNode);
			e.printStackTrace();
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		if(writeRes.getStatus() == error)
			return false;
		return true;
	}
	
	
	private int splitFile(File f, int startIndex) throws IOException {
        //int sizeOfFiles = BLOCK_SIZE;
        char[] buffer = new char[BLOCK_SIZE];
        
        //File fnew = new File("./Clientconfig.properties");
        //try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(f))) {
        try(BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(f), "UTF8"))) {
            String name = f.getName();
            int tmp = 0;
            while ((tmp = br.read(buffer)) > 0) {
                File newFile = new File("./tmp", name + "_" + startIndex++);
                try (FileWriter out = new FileWriter(newFile)) {
                    out.write(buffer, 0, tmp);
                }
            }
        }
        return startIndex-1;
    }
	
	/*private void getNNStub() {
		nameNodeStub = ClientDriver.getNNStub();
	}*/
	
	/*private void getDataNodeStub(int dataNodeNo, String IP, int port) {
		if(dnStubs.containsKey(dataNodeNo))
			return;
		IDataNode stub = ClientDriver.getDNStub(dataNodeNo, IP, port);
		if (stub != null)
			dnStubs.put(dataNodeNo, stub);	
	}*/

	@Override
	public byte[] get(byte[] inp) throws RemoteException {
		return null;
	}

	@Override
	public byte[] put(byte[] inp) throws RemoteException {
		return null;
	}

	@Override
	public byte[] list(byte[] inp) throws RemoteException {
		return null;
	}
	
	public void getNNStub() {
		try {
			nameNodeStub = (INameNode)Naming.lookup("rmi://" + NN_IP + ":" + NN_PORT + "/" + "NN");
		} catch (Exception e) {
			System.out.println("ClientImpl: Error connecting to NN");
		}
	}
	
	public void getDataNodeStub(int dataNodeNo, String DN_IP, int DN_PORT) {
		if(dnStubs.containsKey(dataNodeNo))
			return;
		IDataNode stub = null;
		try {
			stub = (IDataNode)Naming.lookup("rmi://" + DN_IP + ":" + DN_PORT + "/" + "DN" + dataNodeNo);
		} catch (Exception e) {
			System.out.println("DN : " + dataNodeNo + " unreachable!!!");
		}
		if (stub != null)
			dnStubs.put(dataNodeNo, stub);	
	}
}
