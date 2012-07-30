import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
	
	private HashSet<String> knownFailedMachines = new HashSet<String>();
	
	

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
			final HashSet<String> detectedFailedMachines = failureDetector.checkFailures();
			if (detectedFailedMachines.size() > 0) {
				System.out.println("Locally detected new machines: "+detectedFailedMachines+ " and the currently detected failed machines: "+knownFailedMachines);
				Thread detectedFailedMachinesHandler = new Thread(new Runnable() {		
					@Override
					public void run() {
						handleDetectedFailedMachines(detectedFailedMachines);						
					}
				});
				detectedFailedMachinesHandler.start();
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
			return;
		} catch (ClassNotFoundException e) {
			return;
		}
	}
	
	private synchronized void handleHeartbeatQuery(Query query) {
		String IPAddress = query.getQueryOriginMachineIP();
		failureDetector.updateLiveness(IPAddress);
		// If a neighbor has just rejoined
		if (!currentSystemIPList.contains(IPAddress)) {
			reconstructNeighbors();
			storage.changeIsLocalStorage(IPAddress, false);
		}
	}
	
	private synchronized void handleIsNeighborAliveQuery(Query query) {
		System.out.println("handleIsNeighborAliveQuery from IP: "+query.getQueryOriginMachineIP());
		if (!query.getQueryOriginMachineIP().equals(ownIPAddress)) {
			query.setAliveNeighborIP(ownIPAddress);
			sendQuery(query, query.getQueryOriginMachineIP());
			HashSet<String> detectedFailedMachines = query.getFailedMachines();
			System.out.println("Globally detected new machines: "+detectedFailedMachines+ " and the currently detected failed machines: "+knownFailedMachines);
			handleDetectedFailedMachines(detectedFailedMachines);
		} else {
			failureDetector.addAliveNeighbor(query.getAliveNeighborIP());
		}
	}
	
	private synchronized void handleDetectedFailedMachines(HashSet<String> detectedFailedMachines) {
		boolean newFailedMachineDetected = false;
		for (String IPAddress : detectedFailedMachines) {
			if (!knownFailedMachines.contains(detectedFailedMachines)) {
				knownFailedMachines.add(IPAddress);
				newFailedMachineDetected = true;
			}
		}
		if (newFailedMachineDetected) {
			reconstructNeighbors();
			for (String IPAddress : knownFailedMachines) {
				storage.changeIsLocalStorage(IPAddress, true);
			}		
		}
	}
	
	private void reconstructNeighbors() {
		Query query = new Query(QueryType.IS_NEIGHBOR_ALIVE);
		query.setQueryOriginMachineIP(ownIPAddress);
		query.addFailedMachines(knownFailedMachines);
		int neighborNumberToSendQuery = concurrentFailureNumber + 1 > currentSystemIPList.size() - 1 ? 
				currentSystemIPList.size() - 1 : concurrentFailureNumber + 1;
		System.out.println("Sending are you alive queries to "+neighborNumberToSendQuery+ "machines ahead.");		
		sendQueryToMultipleMachines(query, neighborNumberToSendQuery);
		failureDetector.resetAliveNeighbors();
		try {
			Thread.sleep(HEARTBEAT_RECEIVER_WAIT_PERIOD/2);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		HashSet<String> aliveNeighbors = failureDetector.getAliveNeighbors();
		System.out.println("aliveNeighbors: "+aliveNeighbors);
		currentSystemIPList = failureDetector.checkIfNeighborsJoined(currentSystemIPList, initialSystemIPList);
		currentSystemIPList = failureDetector.checkIfNeighborsFailed(currentSystemIPList, ownIPAddress, concurrentFailureNumber);
		setNextNeighborIPAddress();
	}
	
	public void startInsertion(String key, String value) {
		System.out.println("startInsertion");
		ArrayList<String> allValues = storage.lookup(key);
		ArrayList<String> localValues = storage.lookupLocal(key);
		// If the key exists in this machine
		if (allValues != null) {
			// If this machine contains the key but not the value, it inserts the value 
			if (!allValues.contains(value)) {
				storage.insert(key, value);
			}	
			if (localValues != null) {
				System.out.println("(1) The key-value pair <"+key+", "+value+"> has been inserted at the machine with IP: "+ownIPAddress);
			}
		}
		QueryType queryType = QueryType.INSERT_ROUND_1;
		Query query = new Query(queryType, key, value);
		query.setQueryOriginMachineIP(ownIPAddress);
		query.updateStorageSize(ownIPAddress, storage.getLocalStorageSize());
		sendQuery(query, nextNeighborIPAddress);
	}

	public void startLookup(String key) {
		System.out.println("startLookup");
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
		System.out.println("startDeletion");
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
		System.out.println("handleInsertRound1Query");
		// If we are the issuing client
		if (query.getQueryOriginMachineIP().equals(ownIPAddress)) {
			System.out.println("handleInsertRound1Query - 1");
			HashSet<String> machineIPsWithOperation = query.getMachineIPsWithOperation();
			// If a machine in the system detected that it already has the given key and so it inserted the new value
			if (machineIPsWithOperation != null) {
				System.out.println("handleInsertRound1Query - 2");
				String key = query.getKey();
				String value = query.getValue();
				for (String IPAddress : machineIPsWithOperation) {
					System.out.println("(2) The key-value pair <"+key+", "+value+"> has been inserted at the machine with IP: "+IPAddress);
				}
				return;
			}
			// If this machine contains a local or replica storage with lowest load, insert
			if (storage.getStorageKeyIPs().contains(query.getIPWithMininumSize())) {
				System.out.println("handleInsertRound1Query - 3");
				String key = query.getKey();
				String value = query.getValue();
				storage.insert(key, value);
				// If the machine having the lowest load is a local storage in this machine
				if (storage.getLocalStorageIPs().contains(query.getIPWithMininumSize())) {
					System.out.println("(3) The key-value pair <"+key+", "+value+"> has been inserted at the machine with IP: "+ownIPAddress);
				}
			}
			// Start Round 2
			query.setQueryType(QueryType.INSERT_ROUND_2);
			sendQuery(query, nextNeighborIPAddress);
			return;
		}
		String key = query.getKey();
		String value = query.getValue();
		ArrayList<String> allValues = storage.lookup(key);
		ArrayList<String> localValues = storage.lookupLocal(key);
		// If the key exists in this machine
		if (allValues != null) {
			System.out.println("handleInsertRound1Query - 4");
			// If the key exists in this machine but not the value, insert
			if (!allValues.contains(value)) {
				storage.insert(key, value);
			}		
			if (localValues != null) {
				query.addMachineIPWithOperation(ownIPAddress);
			}
			sendQuery(query, nextNeighborIPAddress);
			return;
		}
		// If not, write the storage load and pass the query
		query.updateStorageSize(ownIPAddress, storage.getLocalStorageSize());
		sendQuery(query, nextNeighborIPAddress);
	}

	private void handleInsertRound2Query(Query query) {
		System.out.println("handleInsertRound2Query");
		// If we are the issuing client, announce the machines in which the key-value pair has been inserted
		if (query.getQueryOriginMachineIP().equals(ownIPAddress)) {
			System.out.println("handleInsertRound2Query - 1");
			HashSet<String> machineIPsWithOperation = query.getMachineIPsWithOperation();
			// If a machine in the system detected that it already has the given key and so it inserted the new value
			if (machineIPsWithOperation != null) {
				System.out.println("handleInsertRound2Query - 2");
				String key = query.getKey();
				String value = query.getValue();
				for (String IPAddress : machineIPsWithOperation) {
					System.out.println("(4) The key-value pair <"+key+", "+value+"> has been inserted at the machine with IP: "+IPAddress);
				}
			}
			return;
		}
		// If this machine contains a local or replica storage with lowest load, insert, write it to the query and pass it
		if (storage.getStorageKeyIPs().contains(query.getIPWithMininumSize())) {
			System.out.println("handleInsertRound2Query - 3");
			String key = query.getKey();
			String value = query.getValue();
			storage.insert(key, value);
			// If the machine having the lowest load is a local storage in this machine
			if (storage.getLocalStorageIPs().contains(query.getIPWithMininumSize())) {
				query.addMachineIPWithOperation(ownIPAddress);

			}
			sendQuery(query, nextNeighborIPAddress);
			return;
		}
		// Otherwise, pass the query
		sendQuery(query, nextNeighborIPAddress);
	}
	
	private void handleLookupQuery(Query query) {
		System.out.println("handleLookupQuery");
		String key = query.getKey();
		ArrayList<String> localValues = storage.lookupLocal(key);
		// If we are the issuing client, and if there is a value found for the given key, announce it
		if (query.getQueryOriginMachineIP().equals(ownIPAddress)) {
			HashMap<String, ArrayList<String>> valuesMap = query.getValues();
			Set<String> machineIPsWithOperation = valuesMap.keySet();
			if (machineIPsWithOperation.size() > 0) {
				for (String machineIPWithOperation : machineIPsWithOperation) {
					ArrayList<String> foundValues = valuesMap.get(machineIPWithOperation);
					System.out.println("The following values for the key "+key+" has been found at the machine with IP: "+machineIPWithOperation);
					for (String foundValue : foundValues) {
						System.out.println(foundValue);
					}
				}
			} else {
				System.out.println("A value does not exist in the system for the key: "+key);
			}
			return;
		}
		// If this machine has the value for the given key, write it to the query and pass it
		if (localValues != null) {
			query.addValues(ownIPAddress, localValues);
			sendQuery(query, nextNeighborIPAddress);
			return;
		}
		sendQuery(query, nextNeighborIPAddress);
	}
	
	private void handleDeleteQuery(Query query) {		
		System.out.println("handleDeleteQuery");
		String key = query.getKey();
		ArrayList<String> allValues = storage.lookup(key);
		ArrayList<String> localValues = storage.lookupLocal(key);
		// If we are the issuing client, if there is a value found for the given key and it is deleted, announce it
		if (query.getQueryOriginMachineIP().equals(ownIPAddress)) {
			HashMap<String, ArrayList<String>> valuesMap = query.getValues();
			Set<String> machineIPsWithOperation = valuesMap.keySet();
			if (machineIPsWithOperation.size() > 0) {
				for (String machineIPWithOperation : machineIPsWithOperation) {
					ArrayList<String> foundValues = valuesMap.get(machineIPWithOperation);
					System.out.println("The following values for the key "+key+" has been deleted at the machine with IP: "+machineIPWithOperation);
					for (String foundValue : foundValues) {
						System.out.println(foundValue);
					}				
				}
			} else {
				System.out.println("A value does not exist in the system for the key: "+key);
			}
			return;
		}
		// If this machine has the value for the given key, delete the pair, write it to the query and pass it
		if (allValues != null) {
			storage.delete(key);
			if (localValues != null) {
				query.addValues(ownIPAddress, allValues);
			}
			sendQuery(query, nextNeighborIPAddress);
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
		System.out.println("setNextNeighborIPAddress");
		System.out.println("currentSystemIPList: " + currentSystemIPList);
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