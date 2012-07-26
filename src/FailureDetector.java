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
	
	private long aliveNeighborCheckStartTime;
	
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
	public ArrayList<String> checkFailures() {
		ArrayList<String> failedMachines = new ArrayList<String>();
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
		aliveNeighbors.add(IPAddress);
 	}
	
	public HashSet<String> getAliveNeighbors() {
		return aliveNeighbors;
	}
	
	public void resetAliveNeighbors() {
		Iterator<String> iterator = aliveNeighbors.iterator();
		while (iterator.hasNext()) {
			aliveNeighbors.remove(iterator.next());
		}
		aliveNeighborCheckStartTime = System.currentTimeMillis();
	}
	
	public long getAliveNeighborCheckStartTime() {
		return aliveNeighborCheckStartTime;
	}

}
