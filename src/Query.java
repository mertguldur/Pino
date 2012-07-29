import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;


public class Query implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5185436346460434400L;
	
	private QueryType queryType;
	
	private HashMap<String, Integer> storageSizes = new HashMap<String, Integer>();
	
	private String queryOriginMachineIP;

	private HashSet<String> machineIPsWithOperation;
	
	private String key;
	
	private String value;
		
	private HashMap<String, ArrayList<String>> valuesMap = new HashMap<String, ArrayList<String>>();
	
	private String aliveNeighborIP;
	
	public Query(QueryType queryType) {
		this.queryType = queryType;
	}
	
	public Query(QueryType queryType, String key) {
		this.queryType = queryType;
		this.key = key;
	}
	
	public Query(QueryType queryType, String key, String value) {
		this.queryType = queryType;
		this.key = key;
		this.value = value;
	}
		
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
	
	public String getKey() {
		return key;
	}
	
	public String getValue() {
		return value;
	}
	
	public QueryType getQueryType() {
		return queryType;
	}
	
	public void setQueryType(QueryType queryType) {
		this.queryType = queryType;
	}
	
	public void setQueryOriginMachineIP(String queryOriginMachineIP) {
		this.queryOriginMachineIP = queryOriginMachineIP;
	}
	
	public String getQueryOriginMachineIP() {
		return queryOriginMachineIP;
	}
	
	public void updateStorageSize(String IPAddress, int size) {
		synchronized (storageSizes) {
			storageSizes.put(IPAddress, size);
		}
	}
	
	public void addMachineIPWithOperation(String machineIPWithOperation) {
		if (machineIPsWithOperation == null) {
			machineIPsWithOperation = new HashSet<String>();
 		}
		machineIPsWithOperation.add(machineIPWithOperation);
	}
	
	public HashSet<String> getMachineIPsWithOperation() {
		return machineIPsWithOperation;
	}

	public void setValue(String value) {
		this.value = value;
	}
	
	public void addValues(String IPAddress, ArrayList<String> currentValues) {
		valuesMap.put(IPAddress, currentValues);
	}
	
	public HashMap<String, ArrayList<String>> getValues() {
		return valuesMap;
	}
	
	public void setAliveNeighborIP(String IPAddress) {
		aliveNeighborIP = IPAddress;
	}
	
	public String getAliveNeighborIP() {
		return aliveNeighborIP;
	}
	

}
