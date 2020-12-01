
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Properties;

import dataNode.DataNode;
import interfaces.IDataNode;

public class DNDriver {

	private static int ID = 0;
	private static String IP = "";
	private static int port = 0;
	private static String NN_IP = "";
	private static int NN_PORT = 0;
	
	static {
		try {
			//InetAddress ip = InetAddress.getLocalHost();
			InetAddress.getLocalHost();
			Inet4Address.getLocalHost();
			IP = "localhost";//NetworkInterface.getByName("wlan0").getInetAddresses().nextElement().toString().substring(1);
			//getNetworkInterfaces();
			//IP = ip.getHostAddress();
			System.out.println("Current IP address : " + IP);
			
			Properties prop = new Properties();
			InputStream  input = new FileInputStream("./DNconfig.properties");
			prop.load(input);
			ID = Integer.parseInt(prop.getProperty("ID"));
			port = Integer.parseInt(prop.getProperty("PORT"));
			
			NN_IP = prop.getProperty("NNIP");
			NN_PORT = Integer.parseInt(prop.getProperty("NNPORT"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private Registry registry;
	
	public static void main(String[] args) {
		DNDriver driver = new DNDriver();
		//driver.initailize();
		driver.process();
	}

	/*private void initailize() {
		try {
			InetAddress ip = InetAddress.getLocalHost();
			IP = ip.getHostAddress();
			System.out.println("Current IP address : " + IP);
			
			Properties prop = new Properties();
			InputStream  input = new FileInputStream("./DNconfig.properties");
			prop.load(input);
			ID = Integer.parseInt(prop.getProperty("ID"));
			port = Integer.parseInt(prop.getProperty("PORT"));
			
			NN_IP = prop.getProperty("NNIP");
			NN_PORT = Integer.parseInt(prop.getProperty("NNPORT"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}*/
	
	private void process() {
		startRegistry();
		register();
	}
	
	private void startRegistry() {
		try {
			
			registry = LocateRegistry.createRegistry(port);
		} catch (RemoteException e) {
			System.out.println("Error Starting registry!!");
		}
	}
	
	private void register() {
		try {
			IDataNode nn = new DataNode(ID, IP, port, NN_IP, NN_PORT);
			IDataNode stub = (IDataNode) UnicastRemoteObject.exportObject(nn, 0);
			registry.rebind("DN" + ID, stub);
			//Naming.rebind("rmi://localhost:5000/sonoo",stub);
			//Naming.rebind("rmi://" + IP + ":" + port + "/" + "DN" + ID,stub);
		}catch(Exception e) {
			System.out.println("Error while registering!!");
		}
	}
}
