package dataNode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.protobuf.ByteString;

import interfaces.IDataNode;
import interfaces.INameNode;
import proto.Hdfs.BlockReportRequest;
import proto.Hdfs.BlockReportResponse;
import proto.Hdfs.DNInfo;
import proto.Hdfs.ReadBlockRequest;
import proto.Hdfs.ReadBlockResponse;
import proto.Hdfs.WriteBlockRequest;
import proto.Hdfs.WriteBlockResponse;

public class DataNode implements IDataNode{
	
	private DNInfo.Builder dnInfo = null;
	
	public static final int error = 0;
	public static final int OK = 1;
	public static final String blockStorageInfo = ".//storage//diskBlockEntry";
	public static final String blockStorage = ".//storage//";
	public static final String logStorage = ".//log//DN.log";
	
	
	private Set<Integer> blocks = new HashSet<>();
	private PrintWriter blockDetails = null;
	private static final long TIME_INTERVAL = 4000l;
	private INameNode nameNode = null;
	private PrintWriter logWriter = null;
	private boolean debugWriter = true;
	private int ID = 0;
	private String IP = "";
	private int port = 0;
	
	private String NN_IP = "";
	private int NN_PORT = 0;
	
	public DataNode(int id, String ip, int portNo, String NNIP, int NNPORT) {
		try {
			this.ID = id;
			this.IP = ip;
			this.port = portNo;
			this.NN_IP = NNIP;
			this.NN_PORT = NNPORT;
			logWriter = new PrintWriter(logStorage, "UTF-8");
			dnInfo = DNInfo.newBuilder();
			dnInfo.setDNNumber(ID);
			dnInfo.setIp(IP);
			dnInfo.setPort(port);
			blockDetails = new PrintWriter(new FileOutputStream(blockStorageInfo, true));
			recover();
			getNameNodeStub();
			logWriter.append("\nDN Initialized!!!!" + " IP : " + IP + " PORT : " + port + " ID : " + ID);
			logWriter.flush();
		} catch (Exception e) {
			e.printStackTrace();
		}
		Thread th = new Thread(new ReportThread());
		th.start();
	}
	
	private void recover() throws Exception {
		File f = new File(blockStorageInfo);
		StringBuffer sb = null;
		if (debugWriter) {
			sb = new StringBuffer();
			sb.append("\n**************Started Recovery***********");
		}
		if(f.exists() && !f.isDirectory()) { 
			try (BufferedReader br = new BufferedReader(new FileReader(f))) {
			    String line;
			    while ((line = br.readLine()) != null) {
			    	if (debugWriter) {
			    		sb.append("\n Block Number : " + line);
			    	}
			       int blockNumber = Integer.parseInt(line);
			       blocks.add(blockNumber);
			    }
			}
		}
		
		if (debugWriter) {
			sb.append("******************************");
			/*logWriter.append(sb.toString());
			logWriter.flush();*/
			System.out.println(sb.toString());
		}
	}
	
	/*private void getNameNodeStub() {
		nameNode = DNDriver.getNNStub();
	}*/
	public void getNameNodeStub() {
		try {
			nameNode = (INameNode)Naming.lookup("rmi://" + NN_IP + ":" + NN_PORT + "/" + "NN");
		} catch (Exception e) {
			System.out.println("DNDriver : Error connecting to NN");
		}
	}
	
	class ReportThread implements Runnable {

		@Override
		public void run() {
			sendBlockReport();
		}
		
		private void sendBlockReport() {
			while(true) {
				BlockReportRequest.Builder req = BlockReportRequest.newBuilder();
				req.setId(ID);
				req.setLocation(dnInfo);
				try {
					Thread.sleep(TIME_INTERVAL);
					if (debugWriter) {
						StringBuffer sb = new StringBuffer();
						sb.append("\n***********Blockreport**********");
						sb.append("\n ID : " + ID + " IP : " + dnInfo.getIp() + " PORT : " + dnInfo.getPort());
						sb.append("\n Blocks : " + blocks.toString());
						sb.append("\n******************************");
						System.out.println(sb.toString());
						/*synchronized (logWriter) {
							logWriter.append(sb.toString());
							logWriter.flush();
						}*/
					}
					synchronized (blocks) {
						req.addAllBlockNo(blocks);
					}
					try {
						byte[] resByte = nameNode.blockReport(req.build().toByteArray());
						BlockReportResponse res = BlockReportResponse.parseFrom(resByte);
						if (res != null && res.getStatus() == 0) {
							System.out.println("BlockReport Request for error response!!!");
						}
					} catch (Exception e) {
						System.out.println("Unable to Connect to NN");
						getNameNodeStub();
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}	
		}
	}
	
	@Override
	public byte[] readBlock(byte[] inp) throws RemoteException {
		ReadBlockResponse.Builder res = ReadBlockResponse.newBuilder();
		byte[] data = null;
		try {
			ReadBlockRequest req = ReadBlockRequest.parseFrom(inp);
			int blockNo = req.getBlockNo();
			if (debugWriter) {
				StringBuffer sb = new StringBuffer();
				sb.append("\n*******Read Request **********");
				sb.append("\nFor Block : " + blockNo);
				sb.append("\n******************************");
				System.out.println(sb.toString());
			}
			Path path = Paths.get(blockStorage + blockNo);
			data = Files.readAllBytes(path);
		} catch (Exception e) {
			e.printStackTrace();
			res.setStatus(error);
			return res.build().toByteArray();
		}
		res.addData(ByteString.copyFrom(data));
		res.setStatus(OK);
		return res.build().toByteArray();
	}

	@Override
	public byte[] writeBlock(byte[] inp) throws RemoteException {
		PrintWriter writer = null;
		WriteBlockResponse.Builder res = WriteBlockResponse.newBuilder();
		int blockNumbr = 0;
		try {
			WriteBlockRequest req = WriteBlockRequest.parseFrom(inp);
			blockNumbr = req.getBlockNo();
			List<ByteString> dataList = req.getDataList();
			StringBuffer sb = null;
			if (debugWriter) {
				sb = new StringBuffer();
				sb.append("\n*********Write Block Req***********");
				sb.append("\n Block No : " + blockNumbr);
			}
			writer = new PrintWriter(new FileOutputStream(blockStorage + blockNumbr + "", true));
			
			Iterator<ByteString> it = dataList.iterator();
			
			while (it.hasNext()) {
				ByteString data = it.next();
				writer.append(data.toStringUtf8());
			}
			
			if (debugWriter) {
				sb.append("********************************");
				System.out.println(sb.toString());
				/*synchronized (logWriter) {
					logWriter.append(sb.toString());
				}*/
			}
		} catch (Exception e) {
			e.printStackTrace();
			res.setStatus(error);
			return res.build().toByteArray();
		} finally {
			if (writer != null) {
				try {
					writer.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		synchronized (blocks) {
			blocks.add(blockNumbr);
			blockDetails.append(blockNumbr + "\n");
			blockDetails.flush();
		}
		res.setStatus(OK);
		return res.build().toByteArray();
	}
}
