import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.sql.ConnectionEvent;


// TODO: Auto-generated Javadoc
/**
 *
 * @see ConnectionEvent
 */
public class QueryHandler implements Runnable {
	
	private static final int HEARTBEAT_RECEIVER_WAIT_PERIOD = 10000;
	
	private static final int HEARTBEAT_SENDER_WAIT_PERIOD = 2000;
	
	private FailureDetector failureDetector;
	
	/** The server socket. */
	private ServerSocket serverSocket;
	
	private String nextNeighborIPAddress;
		
	private Storage storage;
		
	private int port;
				
	private String ownIPAddress;
	
	private ArrayList<String> initialSystemIPList;
	
	private ArrayList<String> currentSystemIPList;
		
	private int concurrentFailureNumber;
	
	private boolean reconstructingNeighbors = false;
	
	
	
	/** The states. */	
	/**
	 * Instantiates a new connection listener.
	 *
	 * @param port the port
	 * @param dataFileName 
	 * @param systemIPList 
	 * @param concurrentFailureNumber 
	 * @param waitPeriod the wait period
	 */
	public QueryHandler(int port, String dataFileName, String ownIPAddress, int concurrentFailureNumber, ArrayList<String> systemIPList) {
		try {
			serverSocket = new ServerSocket(port);
		} catch (IOException e) {
			System.out.println("The port number is invalid and the listening socket couldn't be established. " +
					"Quiting the program.");
			System.exit(1);		
		}
		this.port = port;
		this.ownIPAddress = ownIPAddress;
		this.initialSystemIPList = systemIPList;
		this.currentSystemIPList = systemIPList;
		this.concurrentFailureNumber = concurrentFailureNumber;
		setNextNeighborIPAddress();
		initialize(dataFileName);
	}
	
	private void initialize(String dataFileName) {
		Thread queryHandlerThread = new Thread(this);
		queryHandlerThread.start();	
		storage = new Storage(dataFileName, currentSystemIPList, ownIPAddress, concurrentFailureNumber);
		failureDetector = new FailureDetector(HEARTBEAT_RECEIVER_WAIT_PERIOD);
	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		while (true) {
			final Socket receiverSocket = listenConnections();
			if (receiverSocket != null) {
				Thread queryReceiver = new Thread(new Runnable() {			
					@Override
					public void run() {
						receiveQuery(receiverSocket);		
					}
				});
				queryReceiver.start();
			}	
			final ArrayList<String> failedMachines = failureDetector.checkFailures();
			if (failedMachines.size() > 0) {
				Thread failedMachinesHandler = new Thread(new Runnable() {		
					@Override
					public void run() {
						handleFailedMachines(failedMachines);						
					}
				});
				failedMachinesHandler.start();
			}
		}
	}
	
	public void joinToTheSystem() {
		Thread heartbeatSender = new Thread(new Runnable() {
			@Override
			public void run() {
				sendHeartbeats();
			}
		});
		heartbeatSender.start();
	}

	private void sendHeartbeats() {
		while (true) {
			QueryType queryType = QueryType.HEARTBEAT;
			Query query = new Query(queryType);
			query.setQueryOriginMachineIP(ownIPAddress);
			sendQueryToMultipleMachines(query, concurrentFailureNumber);
			try {
				Thread.sleep(HEARTBEAT_SENDER_WAIT_PERIOD);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
 	}

	/**
	 * Listen connections.
	 *
	 * @return the socket
	 */
	private Socket listenConnections() {
		Socket receiverSocket = null;
		try {
			receiverSocket = serverSocket.accept();
		} catch (IOException e) {
			e.printStackTrace();		
		}	
		return receiverSocket;
	}
	
	private void receiveQuery(Socket receiverSocket) {
		try {
			ObjectInputStream objectInputStream = new ObjectInputStream(receiverSocket.getInputStream());
			Query query = (Query) objectInputStream.readObject();
			if (query.getQueryType() == QueryType.INSERT_ROUND_1) {
				handleInsertRound1Query(query);
			}
			else if (query.getQueryType() == QueryType.INSERT_ROUND_2) {
				handleInsertRound2Query(query);
			}
			else if (query.getQueryType() == QueryType.LOOKUP) {
				handleLookupQuery(query);
			}
			else if (query.getQueryType() == QueryType.DELETE) {
				handleDeleteQuery(query);
			}
			else if (query.getQueryType() == QueryType.HEARTBEAT) {
				handleHeartbeatQuery(query);
			}
			else if (query.getQueryType() == QueryType.IS_NEIGHBOR_ALIVE) {
				handleIsNeighborAliveQuery(query);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	private void handleHeartbeatQuery(Query query) {
		String IPAddress = query.getQueryOriginMachineIP();
		failureDetector.updateLiveness(IPAddress);
		// If a neighbor has just rejoined
		if (!currentSystemIPList.contains(IPAddress)) {
			if (!reconstructingNeighbors) {
				reconstructNeighbors();
			}
			storage.changeIsLocalStorage(IPAddress, false);
		}
	}
	
	private void handleFailedMachines(ArrayList<String> failedMachines) {
		if (!reconstructingNeighbors) {
			reconstructNeighbors();
		}
		for (String IPAddress : failedMachines) {
			storage.changeIsLocalStorage(IPAddress, true);
		}		
	}
	
	private void reconstructNeighbors() {
		reconstructingNeighbors = true;
		Query query = new Query(QueryType.IS_NEIGHBOR_ALIVE);
		query.setQueryOriginMachineIP(ownIPAddress);
		sendQueryToMultipleMachines(query, concurrentFailureNumber+1);
		failureDetector.resetAliveNeighbors();
		try {
			Thread.sleep(HEARTBEAT_RECEIVER_WAIT_PERIOD/2);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		HashSet<String> aliveNeighbors = failureDetector.getAliveNeighbors();
		// Check if one or more neighbors rejoined
		for (String aliveNeighborIP : aliveNeighbors) {
			if (!currentSystemIPList.contains(aliveNeighborIP)) {
				currentSystemIPList.add(initialSystemIPList.indexOf(aliveNeighborIP), aliveNeighborIP);
			}
		}
		// Check if one or more neighbors failed
		Iterator<String> iterator = currentSystemIPList.iterator();
		while (iterator.hasNext()) {
			if (!aliveNeighbors.contains(iterator.next())) {
				iterator.remove();
			}
		}
		setNextNeighborIPAddress();
		reconstructingNeighbors = false;
	}
	
	private void handleIsNeighborAliveQuery(Query query) {
		if (!query.getQueryOriginMachineIP().equals(ownIPAddress)) {
			query.setAliveNeighborIP(ownIPAddress);
			sendQuery(query, query.getQueryOriginMachineIP());
			if (!reconstructingNeighbors) {
				reconstructNeighbors();
			}
		} else {
			failureDetector.addAliveNeighbor(query.getAliveNeighborIP());
		}
	}

	public void startInsertion(String key, String value) {
		ArrayList<String> allValues = storage.lookup(key);
		// If the key exists in this machine
		if (allValues != null) {
			// If this machine contains the key but not the value, it inserts the value 
			if (!allValues.contains(value)) {
				storage.insert(key, value);
			}	
			System.out.println("(1) The key-value pair <"+key+", "+value+"> has been inserted at the machine with IP: "+ownIPAddress);
		}
		QueryType queryType = QueryType.INSERT_ROUND_1;
		Query query = new Query(queryType, key, value);
		query.setQueryOriginMachineIP(ownIPAddress);
		query.updateStorageSize(ownIPAddress, storage.getLocalStorageSize());
		sendQuery(query, nextNeighborIPAddress);
	}

	public void startLookup(String key) {
		ArrayList<String> localValues = storage.lookupLocal(key);
		QueryType queryType = QueryType.LOOKUP;
		Query query = new Query(queryType, key);
		query.setQueryOriginMachineIP(ownIPAddress);
		if (localValues != null) {
			query.addValues(ownIPAddress, localValues);
		}
		sendQuery(query, nextNeighborIPAddress);
	}

	public void startDeletion(String key) {
		ArrayList<String> allValues = storage.lookup(key);
		ArrayList<String> localValues = storage.lookupLocal(key);
		QueryType queryType = QueryType.DELETE;
		Query query = new Query(queryType, key);
		query.setQueryOriginMachineIP(ownIPAddress);
		if (allValues != null) {
			storage.delete(key);
			if (localValues != null) {
				query.addValues(ownIPAddress, localValues);
			}
		}
		sendQuery(query, nextNeighborIPAddress);
	}

	private void handleInsertRound1Query(Query query) {
		// If we are the issuing client
		if (query.getQueryOriginMachineIP().equals(ownIPAddress)) {
			HashSet<String> machineIPsWithOperation = query.getMachineIPsWithOperation();
			// If a machine in the system detected that it already has the given key and so it inserted the new value
			if (machineIPsWithOperation != null) {
				String key = query.getKey();
				String value = query.getValue();
				for (String IPAddress : machineIPsWithOperation) {
					System.out.println("(2) The key-value pair <"+key+", "+value+"> has been inserted at the machine with IP: "+IPAddress);
				}
				return;
			}
			// If this machine contains a local or replica storage with lowest load, insert
			if (storage.getStorageKeyIPs().contains(query.getIPWithMininumSize())) {
				String key = query.getKey();
				String value = query.getValue();
				storage.insert(key, value);
				System.out.println("(3) The key-value pair <"+key+", "+value+"> has been inserted at the machine with IP: "+ownIPAddress);
			}
			// Start Round 2
			query.setQueryType(QueryType.INSERT_ROUND_2);
			sendQuery(query, nextNeighborIPAddress);
			return;
		}
		String key = query.getKey();
		String value = query.getValue();
		ArrayList<String> allValues = storage.lookup(key);
		// If the key exists in this machine
		if (allValues != null) {
			// If the key exists in this machine but not the value, insert
			if (!allValues.contains(value)) {
				storage.insert(key, value);
			}		
			query.addMachineIPWithOperation(ownIPAddress);
			sendQuery(query, nextNeighborIPAddress);
			return;
		}
		// If not, write the storage load and pass the query
		query.updateStorageSize(ownIPAddress, storage.getLocalStorageSize());
		sendQuery(query, nextNeighborIPAddress);
	}

	private void handleInsertRound2Query(Query query) {
		// If this machine contains a local or replica storage with lowest load, insert, write it to the query and pass it
		if (storage.getStorageKeyIPs().contains(query.getIPWithMininumSize())) {
			String key = query.getKey();
			String value = query.getValue();
			storage.insert(key, value);
			query.addMachineIPWithOperation(ownIPAddress);
			sendQuery(query, nextNeighborIPAddress);
			return;
		}
		// If we are the issuing client, announce the machines in which the key-value pair has been inserted
		if (query.getQueryOriginMachineIP().equals(ownIPAddress)) {
			HashSet<String> machineIPsWithOperation = query.getMachineIPsWithOperation();
			// If a machine in the system detected that it already has the given key and so it inserted the new value
			if (machineIPsWithOperation != null) {
				String key = query.getKey();
				String value = query.getValue();
				for (String IPAddress : machineIPsWithOperation) {
					System.out.println("(4) The key-value pair <"+key+", "+value+"> has been inserted at the machine with IP: "+IPAddress);
				}
				return;
			}
		}
		// Otherwise, pass the query
		sendQuery(query, nextNeighborIPAddress);
	}
	
	private void handleLookupQuery(Query query) {
		String key = query.getKey();
		ArrayList<String> localValues = storage.lookupLocal(key);
		// If this machine has the value for the given key, write it to the query and pass it
		if (localValues != null) {
			query.addValues(ownIPAddress, localValues);
			sendQuery(query, nextNeighborIPAddress);
			return;
		}
		// If we are the issuing client, and if there is a value found for the given key, announce it
		if (query.getQueryOriginMachineIP().equals(ownIPAddress)) {
			HashMap<String, ArrayList<String>> valuesMap = query.getValues();
			Set<String> machineIPsWithOperation = valuesMap.keySet();
			if (machineIPsWithOperation.size() > 0) {
				for (String machineIPWithOperation : machineIPsWithOperation) {
					ArrayList<String> foundValues = valuesMap.get(machineIPWithOperation);
					System.out.println("The following values for the key "+key+" has been found at the machine with IP: "+machineIPWithOperation);
					System.out.println(foundValues);
				}
			} else {
				System.out.println("A value does not exist in the system for the key: "+key);
			}
			return;
		}
		sendQuery(query, nextNeighborIPAddress);
	}
	
	private void handleDeleteQuery(Query query) {		
		String key = query.getKey();
		ArrayList<String> allValues = storage.lookup(key);
		ArrayList<String> localValues = storage.lookupLocal(key);
		// If this machine has the value for the given key, delete the pair, write it to the query and pass it
		if (allValues != null) {
			storage.delete(key);
			if (localValues != null) {
				query.addValues(ownIPAddress, allValues);
			}
			sendQuery(query, nextNeighborIPAddress);
			return;
		}
		// If we are the issuing client, if there is a value found for the given key and it is deleted, announce it
		if (query.getQueryOriginMachineIP().equals(ownIPAddress)) {
			HashMap<String, ArrayList<String>> valuesMap = query.getValues();
			Set<String> machineIPsWithOperation = valuesMap.keySet();
			if (machineIPsWithOperation.size() > 0) {
				for (String machineIPWithOperation : machineIPsWithOperation) {
					ArrayList<String> foundValues = valuesMap.get(machineIPWithOperation);
					System.out.println("The following values for the key "+key+" has been deleted at the machine with IP: "+machineIPWithOperation);
					System.out.println(foundValues);
				}
			} else {
				System.out.println("A value does not exist in the system for the key: "+key);
			}
			return;
		}
		sendQuery(query, nextNeighborIPAddress);
	}
	
	private void sendQueryToMultipleMachines(Query query, int numberOfMachinesAhead) {
		int size = currentSystemIPList.size();
		int index = (currentSystemIPList.indexOf(ownIPAddress) + 1) % size;
		for (int i=0; i<numberOfMachinesAhead; i++) {
			sendQuery(query, currentSystemIPList.get(index));
			index = (index + 1) % size;
		}
	}
	
	private void sendQuery(Query query, String destinationIPAddress) {
		try {
			Socket socket = new Socket(destinationIPAddress, port);
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
			objectOutputStream.writeObject(query);
			socket.close();
		} catch (UnknownHostException e) {
			return;
		} catch (IOException e) {
			return;
		}
	}
	
	private void setNextNeighborIPAddress() {
		int size = currentSystemIPList.size();
		int index = (currentSystemIPList.indexOf(ownIPAddress)+1) % size;
		nextNeighborIPAddress = currentSystemIPList.get(index);
		System.out.println("The next neighbor is now machine with IP: "+nextNeighborIPAddress);
	}

	public void printStorage() {
		storage.printStorage();
	}

	/**
	 * @return
	 */
	public String getStorageSizes() {
		return storage.getStorageSizesAsString();
	}
	
}
