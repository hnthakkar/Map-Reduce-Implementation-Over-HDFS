import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

import client.ClientImpl;
import interfaces.IClient;

public class ClientDriver {

	private static int ID = 0;
	private static String IP = "";
	private static int port = 0;
	private static String NN_IP = "";
	private static int NN_PORT = 0;
	
	static {
		try {
			InetAddress ip = InetAddress.getLocalHost();
			IP = ip.getHostAddress();
			System.out.println("Current IP address : " + IP);
			
			Properties prop = new Properties();
			InputStream  input = new FileInputStream("./Clientconfig.properties");
			prop.load(input);
			ID = Integer.parseInt(prop.getProperty("ID"));
			port = Integer.parseInt(prop.getProperty("PORT"));
			
			NN_IP = prop.getProperty("NNIP");
			NN_PORT = Integer.parseInt(prop.getProperty("NNPORT"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		ClientDriver driver = new ClientDriver();
		driver.process();
	}
	
	private void process() {
		IClient client = new ClientImpl(NN_IP, NN_PORT);
		System.out.println("Command's \n1.put \n2.get \n3.List \n4.Exit \n>");
		Scanner scan = null;
		try {
			scan = new Scanner(System.in);
			while(true) {
				String line = scan.nextLine();
				String[] inputs = line.split(" ");
				if (inputs.length == 1) {
					if ("List".equalsIgnoreCase(inputs[0])) {
						List<String> files = client.list("./");
						if (files == null) {
							System.out.println("Some Network problem, please try again!!");
							continue;
						}
						System.out.print("\n****************");
						for(String file : files) {
							System.out.print("\n" + file);
						}
						System.out.print("\n****************\n");
					} else if ("Exit".equalsIgnoreCase(inputs[0])) {
						System.exit(0);
					} else {
						System.out.println("Not Supported");
					}
				} else if(inputs.length == 2) {
					String cmd = inputs[0];
					String file = inputs[1];
					if ("put".equalsIgnoreCase(cmd)) {
						int index = file.lastIndexOf("/");
						String path ="";
						String fileName = "";
						if (index != -1) {
							path = file.substring(0,index);
							fileName = file.substring(index+1);
						} else {
							path = ".";
							fileName = file;
						}
						System.out.println("Path : " + path);
						System.out.println("File : " + fileName);
						//String path = "./";

						try {
							System.out.println("Calling put : " + path +"/" + fileName);
							if(client.put(path, file)){
								System.out.println("File Committed");
							} else {
								System.out.println("Problem Committing File");
							}
						} catch (Exception e) {
							System.out.println("Some problem, please try again!!");
						}
					} else if ("get".equalsIgnoreCase(cmd)) {
						try {
							if (client.get(file, true)) {
								System.out.println("File read successfully");
							} else {
								System.out.println("Problem Reading file");
							}
						} catch (Exception e) {
							System.out.println("Some problem, please try again!!");
						}
					} else {
						System.out.println("Not Supported");
					}
				}else {
					System.out.println("Not Supported");
				}
			}	
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			scan.close();
		}
	}
}
