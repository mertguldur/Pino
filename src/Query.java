import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;


// TODO: Auto-generated Javadoc
/**
 * The Class Query.
 */
public class Query implements Serializable {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 5185436346460434400L;
	
	/** The query type. */
	private QueryType queryType;
	
	/** The storage sizes. */
	private HashMap<String, Integer> storageSizes = new HashMap<String, Integer>();
	
	/** The query origin machine ip. */
	private String queryOriginMachineIP;

	/** The machine i ps with operation. */
	private HashSet<String> machineIPsWithOperation;
	
	/** The key. */
	private String key;
	
	/** The value. */
	private String value;
		
	/** The values map. */
	private HashMap<String, ArrayList<String>> valuesMap = new HashMap<String, ArrayList<String>>();
	
	/** The alive neighbor ip. */
	private String aliveNeighborIP;
	
	/** The failed machines. */
	private HashSet<String> failedMachines = new HashSet<String>();
		
	/** The rejoined machine. */
	private String rejoinedMachine;
	
	/** The current system ip list. */
	private ArrayList<String> currentSystemIPList;
	
	/**
	 * Instantiates a new query.
	 *
	 * @param queryType the query type
	 */
	public Query(QueryType queryType) {
		this.queryType = queryType;
	}
	
	/**
	 * Instantiates a new query.
	 *
	 * @param queryType the query type
	 * @param key the key
	 */
	public Query(QueryType queryType, String key) {
		this.queryType = queryType;
		this.key = key;
	}
	
	/**
	 * Instantiates a new query.
	 *
	 * @param queryType the query type
	 * @param key the key
	 * @param value the value
	 */
	public Query(QueryType queryType, String key, String value) {
		this.queryType = queryType;
		this.key = key;
		this.value = value;
	}
		
	/**
	 * Gets the iP with mininum size.
	 *
	 * @return the iP with mininum size
	 */
	public String getIPWithMininumSize() {
		synchronized (storageSizes) {
			Set<String> IPs = storageSizes.keySet();
			int minimumSize = Integer.MAX_VALUE;
			String IPWithMinimumSize = null;
			for (String IP : IPs) {
				if (storageSizes.get(IP) < minimumSize) {
					minimumSize = storageSizes.get(IP);
					IPWithMinimumSize = IP;
				}
			}
			return IPWithMinimumSize;
		}
	}
	
	/**
	 * Gets the key.
	 *
	 * @return the key
	 */
	public String getKey() {
		return key;
	}
	
	/**
	 * Gets the value.
	 *
	 * @return the value
	 */
	public String getValue() {
		return value;
	}
	
	/**
	 * Gets the query type.
	 *
	 * @return the query type
	 */
	public QueryType getQueryType() {
		return queryType;
	}
	
	/**
	 * Sets the query type.
	 *
	 * @param queryType the new query type
	 */
	public void setQueryType(QueryType queryType) {
		this.queryType = queryType;
	}
	
	/**
	 * Sets the query origin machine ip.
	 *
	 * @param queryOriginMachineIP the new query origin machine ip
	 */
	public void setQueryOriginMachineIP(String queryOriginMachineIP) {
		this.queryOriginMachineIP = queryOriginMachineIP;
	}
	
	/**
	 * Gets the query origin machine ip.
	 *
	 * @return the query origin machine ip
	 */
	public String getQueryOriginMachineIP() {
		return queryOriginMachineIP;
	}
	
	/**
	 * Update storage size.
	 *
	 * @param IPAddress the iP address
	 * @param size the size
	 */
	public void updateStorageSize(String IPAddress, int size) {
		synchronized (storageSizes) {
			storageSizes.put(IPAddress, size);
		}
	}
	
	/**
	 * Adds the machine ip with operation.
	 *
	 * @param machineIPWithOperation the machine ip with operation
	 */
	public void addMachineIPWithOperation(String machineIPWithOperation) {
		if (machineIPsWithOperation == null) {
			machineIPsWithOperation = new HashSet<String>();
 		}
		machineIPsWithOperation.add(machineIPWithOperation);
	}
	
	/**
	 * Gets the machine i ps with operation.
	 *
	 * @return the machine i ps with operation
	 */
	public HashSet<String> getMachineIPsWithOperation() {
		return machineIPsWithOperation;
	}

	/**
	 * Sets the value.
	 *
	 * @param value the new value
	 */
	public void setValue(String value) {
		this.value = value;
	}
	
	/**
	 * Adds the values.
	 *
	 * @param IPAddress the iP address
	 * @param currentValues the current values
	 */
	public void addValues(String IPAddress, ArrayList<String> currentValues) {
		for (String existingIPAddress : valuesMap.keySet()) {
			currentValues.removeAll(valuesMap.get(existingIPAddress));
		}
		if (currentValues.size() > 0) {
			valuesMap.put(IPAddress, currentValues);
		}
	}
	
	/**
	 * Gets the values.
	 *
	 * @return the values
	 */
	public HashMap<String, ArrayList<String>> getValues() {
		return valuesMap;
	}
	
	/**
	 * Sets the alive neighbor ip.
	 *
	 * @param IPAddress the new alive neighbor ip
	 */
	public void setAliveNeighborIP(String IPAddress) {
		aliveNeighborIP = IPAddress;
	}
	
	/**
	 * Gets the alive neighbor ip.
	 *
	 * @return the alive neighbor ip
	 */
	public String getAliveNeighborIP() {
		return aliveNeighborIP;
	}
	
	/**
	 * Adds the failed machine.
	 *
	 * @param failedMachine the failed machine
	 */
	public void addFailedMachine(String failedMachine) {
		failedMachines.add(failedMachine);
	}
	
	/**
	 * Adds the failed machines.
	 *
	 * @param failedMachinesToAdd the failed machines to add
	 */
	public void addFailedMachines(HashSet<String> failedMachinesToAdd) {
		failedMachines.addAll(failedMachinesToAdd);
	}
	
	/**
	 * Gets the failed machines.
	 *
	 * @return the failed machines
	 */
	public HashSet<String> getFailedMachines() {
		return failedMachines;
	}

	/**
	 * Gets the rejoined machine.
	 *
	 * @return the rejoined machine
	 */
	public String getRejoinedMachine() {
		return rejoinedMachine;
	}

	/**
	 * Sets the rejoined machine.
	 *
	 * @param rejoinedMachine the new rejoined machine
	 */
	public void setRejoinedMachine(String rejoinedMachine) {
		this.rejoinedMachine = rejoinedMachine;
	}

	/**
	 * Sets the current system ip list.
	 *
	 * @param currentSystemIPList the new current system ip list
	 */
	public void setCurrentSystemIPList(ArrayList<String> currentSystemIPList) {
		this.currentSystemIPList = new ArrayList<String>();
		for (String IPAddress : currentSystemIPList) {
			this.currentSystemIPList.add(IPAddress);
		}
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