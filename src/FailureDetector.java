import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;


public class FailureDetector {
	
	/** The last received message times. */
	private HashMap<String, Long> lastReceivedMessageTimes = new HashMap<String, Long>();
	
	/** The states. */
	private HashMap<String, Boolean> states = new HashMap<String, Boolean>();
	
	private HashSet<String> aliveNeighbors = new HashSet<String>();
		
	private long heartbeatReceiverWaitPeriod;
	
	
	public FailureDetector(long heartbeatReceiverWaitPeriod) {
		this.heartbeatReceiverWaitPeriod = heartbeatReceiverWaitPeriod;
	}
	
	public void updateLiveness(String senderIPAddress) {
		synchronized (lastReceivedMessageTimes) {
			lastReceivedMessageTimes.put(senderIPAddress, System.currentTimeMillis());
		}
		synchronized (states) {
			states.put(senderIPAddress, true);
		}
	}

	/**
	 * Checks failures. Compares the last received heart beat times of computers with the current time.
	 */
	public HashSet<String> checkFailures() {
		HashSet<String> failedMachines = new HashSet<String>();
		long currentTime = System.currentTimeMillis();
		synchronized (lastReceivedMessageTimes) {
			synchronized (states) {
				Set<String> senderIPAddresses = lastReceivedMessageTimes.keySet();
				for (String senderIPAddress : senderIPAddresses) {
					if (currentTime - lastReceivedMessageTimes.get(senderIPAddress) > heartbeatReceiverWaitPeriod && 
						states.containsKey(senderIPAddress) && states.get(senderIPAddress) == true) {
						states.put(senderIPAddress, false);
						failedMachines.add(senderIPAddress);
						System.out.println("The process with IP: "+senderIPAddress+" has crashed.");
					}
				}
			}	
		}
		return failedMachines;
	}
	
	public void addAliveNeighbor(String IPAddress) {
		synchronized (aliveNeighbors) {
			aliveNeighbors.add(IPAddress);
		}
 	}
	
	public HashSet<String> getAliveNeighbors() {
		synchronized (aliveNeighbors) {
			return aliveNeighbors;
		}
	}
	
	public void resetAliveNeighbors() {
		synchronized (aliveNeighbors) {
			aliveNeighbors = new HashSet<String>();
		}
	}
	
	public ArrayList<String> checkIfNeighborsJoined(ArrayList<String> currentSystemIPList, ArrayList<String> initialSystemIPList) {
		synchronized (aliveNeighbors) {
			for (String aliveNeighborIP : aliveNeighbors) {
				if (!currentSystemIPList.contains(aliveNeighborIP)) {
					currentSystemIPList.add(initialSystemIPList.indexOf(aliveNeighborIP), aliveNeighborIP);
				}
			}
			return currentSystemIPList;
		}
	}
	
	public ArrayList<String> checkIfNeighborsFailed(ArrayList<String> currentSystemIPList, String ownIPAddress, int concurrentFailureNumber) {
		synchronized (aliveNeighbors) {
			HashSet<String> failedNeighbors = new HashSet<String>();
			int size = currentSystemIPList.size();
			int index = (currentSystemIPList.indexOf(ownIPAddress) + 1) % size;
			// Check if one or more neighbors failed
			for (int i=0; i<concurrentFailureNumber+1; i++) {
				String neighborIPAddress = currentSystemIPList.get(index);
				if (!aliveNeighbors.contains(neighborIPAddress)) {
					failedNeighbors.add(neighborIPAddress);
				}
				index = (index + 1) % size;
			}
			Iterator<String> iterator = currentSystemIPList.iterator();
			while (iterator.hasNext()) {
				if (failedNeighbors.contains(iterator.next())) {
					iterator.remove();
				}
			}
			return currentSystemIPList;
		}
	}

}