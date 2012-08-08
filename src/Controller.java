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
		
	/** The query handler. */
	private QueryHandler queryHandler;
	
	
	/**
	 * Instantiates a new controller.
	 *
	 * @param port the port
	 * @param dataFileName the data file name
	 * @param concurrentFailureNumber the concurrent failure number
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
		getQueries();
	}

	/**
	 * Read config file.
	 *
	 * @param ownIPAddress the own ip address
	 * @return the array list
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
	
	
	/**
	 * Gets the queries.
	 *
	 * @return the queries
	 */
	private void getQueries() {
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
		System.out.println("Press enter to join to the distributed system.");
		String queryTypeString;
		try {
			queryTypeString = bufferedReader.readLine();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		queryHandler.joinTheSystem();
		while (true) {
			try {
				System.out.println("Enter the type of your query, i.e., \"insert\", \"lookup\", \"delete\", \"print\" or \"size\"");
				queryTypeString = bufferedReader.readLine();
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
					System.out.println(queryHandler.getStorageSizes());
				}
				else if (queryTypeString.equals("current system")) {
					System.out.println(queryHandler.getCurrentSystemIPList());
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