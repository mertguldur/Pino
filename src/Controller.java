import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Scanner;

// TODO: Auto-generated Javadoc
/**
 * The Class Controller.
 *
 * @author guldur1
 */
public class Controller {
		
	private QueryHandler queryHandler;
	
	
	/**
	 * Instantiates a new controller.
	 * @param dataFileName 
	 * @param concurrentFailureNumber 
	 */
	public Controller(int port, String dataFileName, int concurrentFailureNumber) {	
		String ownIPAddress = null;
		try {
			ownIPAddress = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		ArrayList<String> systemIPList = readConfigFile(ownIPAddress);
		queryHandler = new QueryHandler(port, dataFileName, ownIPAddress, concurrentFailureNumber, systemIPList);
		Thread connectionHandlerThread = new Thread(queryHandler);
		connectionHandlerThread.start();			
		getQueries();
	}

	/**
	 * Read config file.
	 * @param ownIPAddress 
	 */
	private ArrayList<String> readConfigFile(String ownIPAddress) {
	    Scanner scanner = null;
	    ArrayList<String> systemIPList = new ArrayList<String>();
		try {
			scanner = new Scanner(new FileInputStream("../IPs.txt"), "UTF-8");
		    while (scanner.hasNextLine()){
		    	systemIPList.add(scanner.nextLine());
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
	      scanner.close();
	    }
		return systemIPList;
	}
	
	
	private void getQueries() {
		while (true) {
			System.out.println("Enter the type of your query, i.e., \"insert\", \"lookup\", \"delete\", \"print\" or \"size\"");
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
			try {
				String queryTypeString = bufferedReader.readLine();
				if (queryTypeString.equals("insert")) {
					System.out.println("Enter the key:");
					String key = bufferedReader.readLine();
					System.out.println("Enter the value:");
					String value = bufferedReader.readLine();
					queryHandler.startInsertion(key, value);
				}
				else if (queryTypeString.equals("lookup")) {
					System.out.println("Enter the key:");
					String key = bufferedReader.readLine();
					queryHandler.startLookup(key);
				}
				else if (queryTypeString.equals("delete")) {
					System.out.println("Enter the key:");
					String key = bufferedReader.readLine();
					queryHandler.startDeletion(key);
				}
				else if (queryTypeString.equals("print")) {
					queryHandler.printStorage();
				}
				else if (queryTypeString.equals("size")) {
					System.out.println("The local storage size is "+queryHandler.getStorageSize());
				}
				else {
					System.out.println("That type of query doesn't exist!");
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}
	

	
	
	
	
}
