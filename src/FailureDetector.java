import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;


// TODO: Auto-generated Javadoc
/**
 * The Class FailureDetector.
 */
public class FailureDetector {
	
	/** The last received message times. */
	private HashMap<String, Long> lastReceivedMessageTimes = new HashMap<String, Long>();
	
	/** The states. */
	private HashMap<String, Boolean> states = new HashMap<String, Boolean>();
	
	/** The alive neighbors. */
	private HashSet<String> aliveNeighbors = new HashSet<String>();
		
	/** The heartbeat receiver wait period. */
	private long heartbeatReceiverWaitPeriod;
	
	
	/**
	 * Instantiates a new failure detector.
	 *
	 * @param heartbeatReceiverWaitPeriod the heartbeat receiver wait period
	 */
	public FailureDetector(long heartbeatReceiverWaitPeriod) {
		this.heartbeatReceiverWaitPeriod = heartbeatReceiverWaitPeriod;
	}
	
	/**
	 * Update liveness.
	 *
	 * @param senderIPAddress the sender ip address
	 */
	public synchronized void updateLiveness(String senderIPAddress) {
		synchronized (lastReceivedMessageTimes) {
			lastReceivedMessageTimes.put(senderIPAddress, System.currentTimeMillis());
		}
		synchronized (states) {
			states.put(senderIPAddress, true);
		}
	}

	/**
	 * Checks failures. Compares the last received heart beat times of computers with the current time.
	 *
	 * @return the hash set
	 */
	public synchronized HashSet<String> checkFailures() {
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
					}
				}
			}	
		}
		return failedMachines;
	}
		
	/**
	 * Removes the additional machine.
	 *
	 * @param rejoinedMachine the rejoined machine
	 */
	public synchronized void stopCheckingMachines(HashSet<String> IPAddresses) {
		for (String IPAddress : IPAddresses) {
			lastReceivedMessageTimes.remove(IPAddress);
			states.remove(IPAddress);
		}
	}
	
	/**
	 * Adds the alive neighbor.
	 *
	 * @param IPAddress the iP address
	 */
	public void addAliveNeighbor(String IPAddress) {
		synchronized (this) {
			aliveNeighbors.add(IPAddress);
		}
 	}
	
	/**
	 * Gets the alive neighbors.
	 *
	 * @return the alive neighbors
	 */
	public HashSet<String> getAliveNeighbors() {
		synchronized (this) {
			return aliveNeighbors;
		}
	}
	
	/**
	 * Reset alive neighbors.
	 */
	public void resetAliveNeighbors() {
		synchronized (this) {
			aliveNeighbors = new HashSet<String>();
		}
	}
	
	/**
	 * Check if neighbors joined.
	 *
	 * @param currentSystemIPList the current system ip list
	 * @param initialSystemIPList the initial system ip list
	 * @return the array list
	 */
	public ArrayList<String> checkIfNeighborsJoined(ArrayList<String> currentSystemIPList, ArrayList<String> initialSystemIPList) {
		synchronized (this) {
			for (String aliveNeighborIP : aliveNeighbors) {
				if (!currentSystemIPList.contains(aliveNeighborIP)) {
					currentSystemIPList.add(initialSystemIPList.indexOf(aliveNeighborIP), aliveNeighborIP);
				}
			}
			return currentSystemIPList;
		}
	}
	
	/**
	 * Check if neighbors failed.
	 *
	 * @param currentSystemIPList the current system ip list
	 * @param ownIPAddress the own ip address
	 * @param concurrentFailureNumber the concurrent failure number
	 * @return the array list
	 */
	public ArrayList<String> checkIfNeighborsFailed(ArrayList<String> currentSystemIPList, String ownIPAddress, int concurrentFailureNumber) {
		synchronized (this) {
			HashSet<String> failedNeighbors = new HashSet<String>();
			int size = currentSystemIPList.size();
			int index = (currentSystemIPList.indexOf(ownIPAddress) + 1) % size;
			int neighborNumberToProcess = concurrentFailureNumber + 1 > currentSystemIPList.size() - 1 ? 
					currentSystemIPList.size() - 1 : concurrentFailureNumber + 1;
			for (int i=0; i<neighborNumberToProcess; i++) {
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