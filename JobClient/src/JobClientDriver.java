import java.io.FileInputStream;
import java.io.InputStream;
import java.rmi.Naming;
import java.util.Properties;
import java.util.Scanner;

import client.ClientImpl;
import helper.SerializeHelper;
import interfaces.IJobTracker;
import proto.MapReduce.JobStatusResponse;
import proto.MapReduce.JobSubmitResponse;

public class JobClientDriver {
	
	private static String JT_IP = "";
	private static int JT_PORT = 0;
	private static String NN_IP = "";
	private static int NN_PORT = 0;
	
	private static final long TIME_INTERVAL = 4000l;
	
	public static final int error = 0;
	public static final int OK = 1;
		
	private IJobTracker JT_Stub = null;
	
	static {
		try {
			Properties prop = new Properties();
			InputStream  input = new FileInputStream("./JCconfig.properties");
			prop.load(input);
			
			JT_IP = prop.getProperty("JTIP");
			JT_PORT = Integer.parseInt(prop.getProperty("JTPORT"));
			
			NN_IP = prop.getProperty("NNIP");
			NN_PORT = Integer.parseInt(prop.getProperty("NNPORT"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	

	public static void main(String[] args) {
		JobClientDriver jc = new JobClientDriver();
		jc.getJTStub();
		jc.process();
	}
	
	protected void process() {
		Scanner scan = null;
		String mapper = null;
		String reducer = null;
		String inpFile = null;
		String outputFile = null;
		int noOfReducer = 0;
		scan = new Scanner(System.in);
		while (true) {
			try {
				System.out.println("Command's format : <mapName> <reducerName> <inputFile in HDFS> <outputFile in HDFS> <numReducers> \n>");
				String line = scan.nextLine();
				String args[] = line.split(" ");
				if (args.length != 5) {
					System.out.println("Please check command format and try again");
					continue;
				}
				
				mapper = args[0];
				reducer = args[1];
				inpFile = args[2];
				outputFile = args[3];
				noOfReducer = Integer.parseInt(args[4]);
				
				byte[] resBytes = null;
				try {
					resBytes = JT_Stub.jobSubmit(SerializeHelper.buildJobSubmitRequest(mapper, reducer, inpFile, outputFile, noOfReducer));
				} catch (Exception e) {
					System.out.println("Problem connecting to JT, please re-try!!");
					getJTStub();
					continue;
				}
				
				if (resBytes == null) {
					System.out.println("Null response from JT");
					continue;
				}
				
				try {
					JobSubmitResponse res = JobSubmitResponse.parseFrom(resBytes);
					if (res.getStatus() == error) {
						System.out.println("Unable to process request!!");
						continue;
					}
					checkStatus(res.getJobId());
					ClientImpl client = new ClientImpl(NN_IP, NN_PORT);
					if(client.get(outputFile, true)) {
						System.out.println("Job Completed, result retrieved from HDFS!!");
					}
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				}
			} catch (Exception e) {
				e.printStackTrace();
			} 
		}	
	}
	
	private void checkStatus(int jobID) {
		if (jobID < 1){
			System.out.println("Invalid JobID");
			return;
		}
		
		while (true) {
			try {
				Thread.sleep(TIME_INTERVAL);
				byte[] resBytes = JT_Stub.getJobStatus(SerializeHelper.buildJobStatusRequest(jobID));
				
				if (resBytes == null) {
					System.out.println("Null response from JT");
				}
				
				JobStatusResponse res = JobStatusResponse.parseFrom(resBytes);
				if (res.getStatus() == error) {
					System.out.println("Unable to get the Job status");
					continue;
				}
				
				if (res.getJobDone()) {
					System.out.println("\n**********************\n       JOB COMPLETED         \n************************\n");
					break;
				}
				
				System.out.print("\n*************************\n");
				System.out.print("\nTotal Map Tasks : " + res.getTotalMapTasks());
				System.out.print("\nTotal Map Task Started : " + res.getNumMapTasksStarted());
				System.out.print("\nTotal Reduce Tasks : " + res.getTotalReduceTasks());
				System.out.print("\nTotal Map Task Started : " + res.getNumReduceTasksStarted());
				System.out.print("\n*************************\n");
			} catch (Exception e) {
				System.out.println("Problem connecting to JT, for getting status");
				getJTStub();
			}
		}
	}
	
	private void getJTStub() {
		try {
			JT_Stub = (IJobTracker)Naming.lookup("rmi://" + JT_IP + ":" + JT_PORT + "/" + "JT");
		} catch (Exception e) {
			System.out.println("JobClient : Error connecting to JT");
			e.printStackTrace();
		}
	}
}
