import nameNode.*;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import interfaces.INameNode;

public class NNDriver {

	private static final int port = 9991;
	private Registry registry;
	
	public static void main(String[] args) {
		NNDriver driver = new NNDriver();
		driver.process();
	}

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
			INameNode nn = new NameNode();
			INameNode stub = (INameNode) UnicastRemoteObject.exportObject(nn, 0);
			registry.rebind("NN", stub);
		}catch(Exception e) {
			System.out.println("Error while registering!!");
		}
	}
}
