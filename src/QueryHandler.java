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
	
	private ArrayList<String> systemIPList;
	
	private ArrayList<String> builtLocalStorages = new ArrayList<String>();
	
	private int concurrentFailureNumber;
	
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
		this.systemIPList = systemIPList;
		this.concurrentFailureNumber = concurrentFailureNumber;
		setNextNeighborIPAddress();
		initialize(dataFileName);
	}
	
	private void initialize(String dataFileName) {
		Thread queryHandlerThread = new Thread(this);
		queryHandlerThread.start();	
		storage = new Storage(dataFileName, systemIPList, ownIPAddress);
		QueryType queryType = QueryType.BUILT_LOCAL_STORAGE;
		Query query = new Query(queryType);
		query.setQueryOriginMachineIP(ownIPAddress);
		sendQuery(query, nextNeighborIPAddress);
		failureDetector = new FailureDetector(HEARTBEAT_RECEIVER_WAIT_PERIOD);
		Thread heartbeatSender = new Thread(new Runnable() {
			@Override
			public void run() {
				sendHeartbeats();
			}
		});
		heartbeatSender.start();
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
			ArrayList<String> failedMachines = failureDetector.checkFailures();
			if (failedMachines.size() > 0) {
				reconstructNeighbors();
			}
		}
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
			serverSocket.setSoTimeout(HEARTBEAT_RECEIVER_WAIT_PERIOD);
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
				failureDetector.updateLiveness(query.getQueryOriginMachineIP());
			}
			else if (query.getQueryType() == QueryType.BUILT_LOCAL_STORAGE) {
				updateBuiltLocalStoragesInfo(query);
			}
			else if (query.getQueryType() == QueryType.REPLICA_TRANSFER) {
				insertReplicaStorage(query);
			}
			else if (query.getQueryType() == QueryType.INSERT_INTO_REPLICA) {
				handleInsertIntoReplicaQuery(query);
			}
			else if (query.getQueryType() == QueryType.DELETE_FROM_REPLICA) {
				handleDeleteFromReplicaQuery(query);
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

	private void updateBuiltLocalStoragesInfo(Query query) {
		builtLocalStorages.add(query.getQueryOriginMachineIP());
		if (!query.getQueryOriginMachineIP().equals(ownIPAddress)) {
			sendQuery(query, nextNeighborIPAddress);						
		}
		if (builtLocalStorages.size() == systemIPList.size()) {
			startSendingReplicas();
		}
	}
	private void startSendingReplicas() {
		QueryType queryType = QueryType.REPLICA_TRANSFER;
		Query query = new Query(queryType);
		query.setQueryOriginMachineIP(ownIPAddress);
		query.setReplicaStorage(storage.getLocalStorage());
		sendQueryToMultipleMachines(query, concurrentFailureNumber);
	}

	private void insertReplicaStorage(Query query) {
		String IPAddress = query.getQueryOriginMachineIP();
		HashMap<String, ArrayList<String>> replicaStorage = query.getReplicaStorage();
		storage.insertReplicaStorage(IPAddress, replicaStorage);
	}
	
	private void reconstructNeighbors() {
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
		Iterator<String> iterator = systemIPList.iterator();
		while (iterator.hasNext()) {
			if (!aliveNeighbors.contains(iterator.next())) {
				iterator.remove();
			}
		}
		setNextNeighborIPAddress();
	}
	
	private void handleIsNeighborAliveQuery(Query query) {
		if (!query.getQueryOriginMachineIP().equals(ownIPAddress)) {
			query.setAliveNeighborIP(ownIPAddress);
			sendQuery(query, query.getQueryOriginMachineIP());
		} else {
			failureDetector.addAliveNeighbor(query.getAliveNeighborIP());
		}
	}

	public void startInsertion(String key, String value) {
		ArrayList<String> currentValues = storage.lookup(ownIPAddress, key);
		// If the key exists in this machine
		if (currentValues != null) {
			// Returns immediately if the key-value pair exists in this machine
			if (currentValues.contains(value)) {
				System.out.println("The key-value pair <"+key+", "+value+"> has been inserted at the machine with IP: "+ownIPAddress);
			}	
			// If this machine contains the key but not the value, it inserts the value locally and informs the machines with replicas
			else {
				storage.insert(ownIPAddress, key, value);
				informReplicasAboutStorageUpdate(QueryType.INSERT_INTO_REPLICA, key, value);
				System.out.println("The key-value pair <"+key+", "+value+"> has been inserted at the machine with IP: "+ownIPAddress);
			}	
			return;
		}
		QueryType queryType = QueryType.INSERT_ROUND_1;
		Query query = new Query(queryType, key, value);
		query.setQueryOriginMachineIP(ownIPAddress);
		query.updateStorageSize(ownIPAddress, storage.getSize());
		sendQuery(query, nextNeighborIPAddress);
	}

	public void startLookup(String key) {
		ArrayList<String> currentValues = storage.lookup(ownIPAddress, key);
		QueryType queryType = QueryType.LOOKUP;
		Query query = new Query(queryType, key);
		query.setQueryOriginMachineIP(ownIPAddress);
		if (currentValues != null) {
			query.addValues(ownIPAddress, currentValues);
		}
		sendQuery(query, nextNeighborIPAddress);
	}

	public void startDeletion(String key) {
		ArrayList<String> currentValues = storage.lookup(ownIPAddress, key);
		QueryType queryType = QueryType.DELETE;
		Query query = new Query(queryType, key);
		query.setQueryOriginMachineIP(ownIPAddress);
		if (currentValues != null) {
			storage.delete(ownIPAddress, key);
			informReplicasAboutStorageUpdate(QueryType.DELETE_FROM_REPLICA, key, null);
			query.addValues(ownIPAddress, currentValues);
		}
		sendQuery(query, nextNeighborIPAddress);
	}

	private void handleInsertRound1Query(Query query) {
		// If we are the issuing client
		if (query.getQueryOriginMachineIP().equals(ownIPAddress)) {
			String machineIPWithOperation = query.getMachineIPWithOperation();
			// If a machine in the system detected that it already has the given key and so it inserted the new value
			if (machineIPWithOperation != null) {
				String key = query.getKey();
				String value = query.getValue();
				System.out.println("The key-value pair <"+key+", "+value+"> has been inserted at the machine with IP: "+machineIPWithOperation);
				return;
			}
			// If this machine has the lowest load, there is no need for round 2
			if (query.getIPWithMininumSize().equals(ownIPAddress)) {
				String key = query.getKey();
				String value = query.getValue();
				storage.insert(ownIPAddress, key, value);
				informReplicasAboutStorageUpdate(QueryType.INSERT_INTO_REPLICA, key, value);
				System.out.println("The key-value pair <"+key+", "+value+"> has been inserted at the machine with IP: "+ownIPAddress);
				return;
			}
			// Otherwise, start Round 2
			query.setQueryType(QueryType.INSERT_ROUND_2);
			sendQuery(query, nextNeighborIPAddress);
			return;
		}
		String key = query.getKey();
		String value = query.getValue();
		ArrayList<String> currentValues = storage.lookup(ownIPAddress, key);
		// If the key exists in this machine
		if (currentValues != null) {
			// if the key-value pair exists in this machine
			if (currentValues.contains(value)) {
				query.setMachineIPWithOperation(ownIPAddress);
			}	
			// If this machine contains the key but not the value, it inserts the value locally and informs the machines with replicas
			else {
				storage.insert(ownIPAddress, key, value);
				informReplicasAboutStorageUpdate(QueryType.INSERT_INTO_REPLICA, key, value);
			}	
			query.setMachineIPWithOperation(ownIPAddress);
			sendQuery(query, nextNeighborIPAddress);
			return;
		}
		// If not, write the storage load and pass the query
		query.updateStorageSize(ownIPAddress, storage.getSize());
		sendQuery(query, nextNeighborIPAddress);
	}

	private void handleInsertRound2Query(Query query) {
		// If this machine has the lowest load, write it to the query and pass it
		if (query.getIPWithMininumSize().equals(ownIPAddress)) {
			String key = query.getKey();
			String value = query.getValue();
			storage.insert(ownIPAddress, key, value);
			informReplicasAboutStorageUpdate(QueryType.INSERT_INTO_REPLICA, key, value);
			query.setMachineIPWithOperation(ownIPAddress);
			sendQuery(query, nextNeighborIPAddress);
			return;
		}
		// If we are the issuing client, announce the machine in which the key-value pair has been inserted
		if (query.getQueryOriginMachineIP().equals(ownIPAddress)) {
			String machineIPWithOperation = query.getMachineIPWithOperation();
			String key = query.getKey();
			String value = query.getValue();
			System.out.println("The key-value pair <"+key+", "+value+"> has been inserted at the machine with IP: "+machineIPWithOperation);
			return;
		}
		// Otherwise, pass the query
		sendQuery(query, nextNeighborIPAddress);
	}
	
	private void handleLookupQuery(Query query) {
		String key = query.getKey();
		ArrayList<String> currentValues = storage.lookup(ownIPAddress, key);
		// If this machine has the value for the given key, write it to the query and pass it
		if (currentValues != null) {
			query.addValues(ownIPAddress, currentValues);
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
		ArrayList<String> currentValues = storage.lookup(ownIPAddress, key);
		// If this machine has the value for the given key, delete the pair, write it to the query and pass it
		if (currentValues != null) {
			storage.delete(ownIPAddress, key);
			informReplicasAboutStorageUpdate(QueryType.DELETE_FROM_REPLICA, key, null);
			query.addValues(ownIPAddress, currentValues);
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
	
	private void handleInsertIntoReplicaQuery(Query query) {
		String IPAddress = query.getQueryOriginMachineIP();
		String key = query.getKey();
		String value = query.getValue();
		storage.insert(IPAddress, key, value);
	}
	
	private void handleDeleteFromReplicaQuery(Query query) {
		String IPAddress = query.getQueryOriginMachineIP();
		String key = query.getKey();
		storage.delete(IPAddress, key);
	}
	
	private void informReplicasAboutStorageUpdate(QueryType queryType, String key, String value) {
		Query query = new Query(queryType, key, value);
		query.setQueryOriginMachineIP(ownIPAddress);
		sendQueryToMultipleMachines(query, concurrentFailureNumber);
	}
	
	private void sendQueryToMultipleMachines(Query query, int numberOfMachinesAhead) {
		int size = systemIPList.size();
		int index = (systemIPList.indexOf(ownIPAddress) + 1) % size;
		for (int i=0; i<numberOfMachinesAhead; i++) {
			sendQuery(query, systemIPList.get(index));
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
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void setNextNeighborIPAddress() {
		int size = systemIPList.size();
		int index = (systemIPList.indexOf(ownIPAddress)+1) % size;
		nextNeighborIPAddress = systemIPList.get(index);
	}

	public void printStorage() {
		storage.printStorage();
	}

	/**
	 * @return
	 */
	public int getStorageSize() {
		return storage.getSize();
	}
	
}
