package interfaces;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface IClient extends Remote{

	byte[] get(byte[] inp) throws RemoteException;
	byte[] put(byte[] inp) throws RemoteException;	
	byte[] list(byte[] inp) throws RemoteException;
	boolean get(String fileName, boolean forRead) throws RemoteException;
	boolean put(String path, String fileName) throws RemoteException;	
	List<String> list(String dirName) throws RemoteException;
}
