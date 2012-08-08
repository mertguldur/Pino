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
 * The Class QueryHandler.
 *
 * @see ConnectionEvent
 */
public class QueryHandler implements Runnable {
	
	/** The Constant HEARTBEAT_RECEIVER_WAIT_PERIOD. */
	private static final int HEARTBEAT_RECEIVER_WAIT_PERIOD = 10000;
	
	/** The Constant HEARTBEAT_SENDER_WAIT_PERIOD. */
	private static final int HEARTBEAT_SENDER_WAIT_PERIOD = 1000;
	
	/** The failure detector. */
	private FailureDetector failureDetector;
	
	/** The server socket. */
	private ServerSocket serverSocket;
	
	/** The next neighbor ip address. */
	private String nextNeighborIPAddress;
		
	/** The storage. */
	private Storage storage;
		
	/** The port. */
	private int port;
				
	/** The own ip address. */
	private String ownIPAddress;
	
	/** The initial system ip list. */
	private ArrayList<String> initialSystemIPList = new ArrayList<String>();
	
	/** The current system ip list. */
	private ArrayList<String> currentSystemIPList;
		
	/** The concurrent failure number. */
	private int concurrentFailureNumber;
	
	/** The known failed machines. */
	private HashSet<String> knownFailedMachines = new HashSet<String>();
	
	
	/**
	 * Instantiates a new query handler.
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
		for (String IPAddress : systemIPList) {
			initialSystemIPList.add(IPAddress);
		}
		this.currentSystemIPList = systemIPList;
		this.concurrentFailureNumber = concurrentFailureNumber;
		setNextNeighborIPAddress();
		initialize(dataFileName);
	}
	
	/**
	 * Initialize.
	 *
	 * @param dataFileName the data file name
	 */
	private void initialize(String dataFileName) {
		failureDetector = new FailureDetector(HEARTBEAT_RECEIVER_WAIT_PERIOD);
		Thread queryHandlerThread = new Thread(this);
		queryHandlerThread.start();	
		storage = new Storage(dataFileName, currentSystemIPList, ownIPAddress, concurrentFailureNumber);
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
			Thread detectedFailedMachinesHandler = new Thread(new Runnable() {		
				@Override
				public void run() {
					final HashSet<String> detectedFailedMachines = failureDetector.checkFailures();
					if (detectedFailedMachines.size() > 0) {
						if(!isPredecessorFailure(detectedFailedMachines)) {
							failureDetector.stopCheckingMachines(detectedFailedMachines);
							return;
						}
						System.out.println("The process with IPs: "+detectedFailedMachines+" has failed.");
						handleDetectedFailedMachines(detectedFailedMachines);						
					}
				}
			});
			detectedFailedMachinesHandler.start();
		}
	}
	
	/**
	 * Join to the system.
	 */
	public void joinTheSystem() {
		Thread heartbeatSender = new Thread(new Runnable() {
			@Override
			public void run() {
				sendHeartbeats();
			}
		});
		heartbeatSender.start();
	}

	/**
	 * Send heartbeats.
	 */
	private void sendHeartbeats() {
		while (true) {
			QueryType queryType = QueryType.HEARTBEAT;
			Query query = new Query(queryType);
			query.setQueryOriginMachineIP(ownIPAddress);
			int neighborNumberToSendQuery = concurrentFailureNumber > currentSystemIPList.size() - 1 ? 
					currentSystemIPList.size() - 1 : concurrentFailureNumber;
			sendQueryToMultipleMachines(query, neighborNumberToSendQuery);
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
	
	/**
	 * Receive query.
	 *
	 * @param receiverSocket the receiver socket
	 */
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
	
	/**
	 * Checks if is predecessor failure.
	 *
	 * @param detectedFailedMachines the detected failed machines
	 * @return true, if is predecessor failure
	 */
	private boolean isPredecessorFailure(HashSet<String> detectedFailedMachines) {
		int neighborNumberToProcess = concurrentFailureNumber > currentSystemIPList.size() - 1 ? 
				currentSystemIPList.size() - 1 : concurrentFailureNumber;
		int index = currentSystemIPList.indexOf(ownIPAddress) - 1; 		
		for (int i=0; i<neighborNumberToProcess; i++) {
			if (index < 0) {
				index += currentSystemIPList.size();
			}
			if (detectedFailedMachines.contains(currentSystemIPList.get(index))) {
				return true;
			}
			index--;
		}
		return false;
	}

	/**
	 * Handle heartbeat query.
	 *
	 * @param query the query
	 */
	private void handleHeartbeatQuery(Query query) {
		String IPAddress = query.getQueryOriginMachineIP();
		failureDetector.updateLiveness(IPAddress);
		// If a neighbor has just rejoined
		if (!currentSystemIPList.contains(IPAddress)) {
			System.out.println("The process with IP: "+IPAddress+" has rejoined.");
			handleDetectedRejoinedMachine(IPAddress, null);
		}
	}
	
	/**
	 * Handle is neighbor alive query.
	 *
	 * @param query the query
	 */
	private void handleIsNeighborAliveQuery(Query query) {
		if (!query.getQueryOriginMachineIP().equals(ownIPAddress)) {
			query.setAliveNeighborIP(ownIPAddress);
			sendQuery(query, query.getQueryOriginMachineIP());
			HashSet<String> detectedFailedMachines = query.getFailedMachines();
			if (query.getRejoinedMachine() != null) {
				handleDetectedRejoinedMachine(query.getRejoinedMachine(), query.getFailedMachines());
			} else {
				handleDetectedFailedMachines(detectedFailedMachines);
			}
		} else {
			failureDetector.addAliveNeighbor(query.getAliveNeighborIP());
		}
	}
	
	/**
	 * Handle detected rejoined machine.
	 *
	 * @param rejoinedIPAddress the rejoined ip address
	 * @param detectedFailedMachines the detected failed machines
	 */
	private synchronized void handleDetectedRejoinedMachine(String rejoinedIPAddress, HashSet<String> detectedFailedMachines) {
		if (detectedFailedMachines != null) {
			knownFailedMachines.addAll(detectedFailedMachines);
			currentSystemIPList.removeAll(detectedFailedMachines);
			for (String IPAddress : detectedFailedMachines) {
				storage.changeIsLocalStorage(IPAddress, true);
			}
			setNextNeighborIPAddress();
		}
		boolean newRejoinedMachineDetected = false;
		Iterator<String> iterator = knownFailedMachines.iterator();
		while (iterator.hasNext()) {
			if (iterator.next().equals(rejoinedIPAddress)) {
				newRejoinedMachineDetected = true;
				iterator.remove();
			}
		}
		if (newRejoinedMachineDetected) {
			reconstructNeighbors(rejoinedIPAddress);
		}
	}

	/**
	 * Handle detected failed machines.
	 *
	 * @param detectedFailedMachines the detected failed machines
	 */
	private synchronized void handleDetectedFailedMachines(HashSet<String> detectedFailedMachines) {
		boolean newFailedMachineDetected = false;
		for (String IPAddress : detectedFailedMachines) {
			if (!knownFailedMachines.contains(IPAddress)) {
				knownFailedMachines.add(IPAddress);
				newFailedMachineDetected = true;
			}
		}
		if (newFailedMachineDetected) {
			reconstructNeighbors(null);
			for (String IPAddress : detectedFailedMachines) {
				storage.changeIsLocalStorage(IPAddress, true);
			}		
		}
	}
	
	/**
	 * Reconstruct neighbors.
	 *
	 * @param rejoinedIPAddress the rejoined ip address
	 */
	private void reconstructNeighbors(String rejoinedIPAddress) {
		Query query = new Query(QueryType.IS_NEIGHBOR_ALIVE);
		query.setQueryOriginMachineIP(ownIPAddress);
		if (rejoinedIPAddress != null) {
			query.setRejoinedMachine(rejoinedIPAddress);
			if (!currentSystemIPList.contains(rejoinedIPAddress)) {
				readdToCurrentSystemIPList(rejoinedIPAddress);
			}
		}
		query.addFailedMachines(knownFailedMachines);
		int neighborNumberToSendQuery = concurrentFailureNumber + 1 > currentSystemIPList.size() - 1 ? 
				currentSystemIPList.size() - 1 : concurrentFailureNumber + 1;
		sendQueryToMultipleMachines(query, neighborNumberToSendQuery);
		failureDetector.resetAliveNeighbors();
		try {
			Thread.sleep(HEARTBEAT_RECEIVER_WAIT_PERIOD/2);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		HashSet<String> aliveNeighbors = failureDetector.getAliveNeighbors();
		currentSystemIPList = failureDetector.checkIfNeighborsJoined(currentSystemIPList, initialSystemIPList);
		currentSystemIPList = failureDetector.checkIfNeighborsFailed(currentSystemIPList, ownIPAddress, concurrentFailureNumber);
		Iterator<String> iterator = knownFailedMachines.iterator();
		while (iterator.hasNext()) {
			if (aliveNeighbors.contains(iterator.next())) {
				iterator.remove();
			}
		}
		setNextNeighborIPAddress();
	}
	
	/**
	 * Readd to current system ip list.
	 *
	 * @param rejoinedIPAddress the rejoined ip address
	 */
	private void readdToCurrentSystemIPList(String rejoinedIPAddress) {
		int livingSuccessorIndex = 0;
		String livingSuccessor = null;
		int currentMachineIndex = initialSystemIPList.indexOf(rejoinedIPAddress);
		for (int i=0; i<initialSystemIPList.size(); i++) {
			currentMachineIndex++;
			livingSuccessorIndex = currentMachineIndex % initialSystemIPList.size();
			livingSuccessor = initialSystemIPList.get(livingSuccessorIndex);
			if (currentSystemIPList.contains(livingSuccessor)) {
				break;
			}
		}
		currentSystemIPList.add(currentSystemIPList.indexOf(livingSuccessor), rejoinedIPAddress);
	}

	/**
	 * Start insertion.
	 *
	 * @param key the key
	 * @param value the value
	 */
	public void startInsertion(String key, String value) {
		ArrayList<String> allValues = storage.lookup(key);
		ArrayList<String> localValues = storage.lookupLocal(key);
		// If the key exists in this machine
		if (allValues != null) {
			// If this machine contains the key but not the value, it inserts the value 
			if (!allValues.contains(value)) {
				storage.insert(key, value);
			}	
			if (localValues != null) {
				System.out.println("The key-value pair <"+key+", "+value+"> has been inserted at the machine with IP: "+ownIPAddress);
			}
		}
		QueryType queryType = QueryType.INSERT_ROUND_1;
		Query query = new Query(queryType, key, value);
		query.setQueryOriginMachineIP(ownIPAddress);
		query.updateStorageSize(ownIPAddress, storage.getLocalStorageSize());
		sendQuery(query, nextNeighborIPAddress);
	}

	/**
	 * Start lookup.
	 *
	 * @param key the key
	 */
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

	/**
	 * Start deletion.
	 *
	 * @param key the key
	 */
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

	/**
	 * Handle insert round1 query.
	 *
	 * @param query the query
	 */
	private void handleInsertRound1Query(Query query) {
		// If we are the issuing client
		if (query.getQueryOriginMachineIP().equals(ownIPAddress)) {
			HashSet<String> machineIPsWithOperation = query.getMachineIPsWithOperation();
			// If a machine in the system detected that it already has the given key and so it inserted the new value
			if (machineIPsWithOperation != null) {
				String key = query.getKey();
				String value = query.getValue();
				for (String IPAddress : machineIPsWithOperation) {
					System.out.println("The key-value pair <"+key+", "+value+"> has been inserted at the machine with IP: "+IPAddress);
				}
				return;
			}
			// If this machine contains a local or replica storage with lowest load, insert
			if (storage.getStorageKeyIPs().contains(query.getIPWithMininumSize())) {
				String key = query.getKey();
				String value = query.getValue();
				storage.insert(key, value);
				// If the machine having the lowest load is a local storage in this machine
				if (storage.getLocalStorageIPs().contains(query.getIPWithMininumSize())) {
					System.out.println("The key-value pair <"+key+", "+value+"> has been inserted at the machine with IP: "+ownIPAddress);
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

	/**
	 * Handle insert round2 query.
	 *
	 * @param query the query
	 */
	private void handleInsertRound2Query(Query query) {
		// If we are the issuing client, announce the machines in which the key-value pair has been inserted
		if (query.getQueryOriginMachineIP().equals(ownIPAddress)) {
			HashSet<String> machineIPsWithOperation = query.getMachineIPsWithOperation();
			// If a machine in the system detected that it already has the given key and so it inserted the new value
			if (machineIPsWithOperation != null) {
				String key = query.getKey();
				String value = query.getValue();
				for (String IPAddress : machineIPsWithOperation) {
					System.out.println("The key-value pair <"+key+", "+value+"> has been inserted at the machine with IP: "+IPAddress);
				}
			}
			return;
		}
		// If this machine contains a local or replica storage with lowest load, insert, write it to the query and pass it
		if (storage.getStorageKeyIPs().contains(query.getIPWithMininumSize())) {
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
	
	/**
	 * Handle lookup query.
	 *
	 * @param query the query
	 */
	private void handleLookupQuery(Query query) {
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
	
	/**
	 * Handle delete query.
	 *
	 * @param query the query
	 */
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
	
	/**
	 * Send query to multiple machines.
	 *
	 * @param query the query
	 * @param numberOfMachinesAhead the number of machines ahead
	 */
	private void sendQueryToMultipleMachines(Query query, int numberOfMachinesAhead) {
		int size = currentSystemIPList.size();
		int index = (currentSystemIPList.indexOf(ownIPAddress) + 1) % size;
		for (int i=0; i<numberOfMachinesAhead; i++) {
			sendQuery(query, currentSystemIPList.get(index));
			index = (index + 1) % size;
		}
	}
	
	/**
	 * Send query.
	 *
	 * @param query the query
	 * @param destinationIPAddress the destination ip address
	 */
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
	
	/**
	 * Sets the next neighbor ip address.
	 */
	private void setNextNeighborIPAddress() {
		System.out.println("Current System: " + currentSystemIPList);
		int size = currentSystemIPList.size();
		int index = (currentSystemIPList.indexOf(ownIPAddress)+1) % size;
		nextNeighborIPAddress = currentSystemIPList.get(index);
		System.out.println("The next neighbor is now machine with IP: "+nextNeighborIPAddress);
	}

	/**
	 * Prints the storage.
	 */
	public void printStorage() {
		storage.printStorage();
	}

	/**
	 * Gets the storage sizes.
	 *
	 * @return the storage sizes
	 */
	public String getStorageSizes() {
		return storage.getStorageSizesAsString();
	}
	
	/**
	 * Gets the current system ip list.
	 *
	 * @return the current system ip list
	 */
	public ArrayList<String> getCurrentSystemIPList() {
		return currentSystemIPList;
	}
	
}