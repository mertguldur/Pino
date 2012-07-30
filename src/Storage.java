import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;


public class Storage {
	
	private HashMap<String, HashMap<String, ArrayList<String>>> storage = new HashMap<String, HashMap<String, ArrayList<String>>>();
	
	private HashSet<String> localStorageIPs = new HashSet<String>();
	
	private String dataFileName;
	
	private String ownIPAddress;
	
	private int concurrentFailureNumber;
	
	public Storage(String dataFileName, ArrayList<String> systemIPList, String ownIPAddress, int concurrentFailureNumber) {
		this.dataFileName = dataFileName;
		this.ownIPAddress = ownIPAddress;
		this.concurrentFailureNumber = concurrentFailureNumber;
		localStorageIPs.add(ownIPAddress);
		buildStorage(systemIPList);
	}
	
	private void buildStorage(ArrayList<String> systemIPList) {
		int totalLineNumber = getLineNumberOfFile();
		float index = systemIPList.indexOf(ownIPAddress);
		float size = systemIPList.size();
		for (int i=0; i<concurrentFailureNumber+1; i++) {
			final String IPAddress = systemIPList.get((int) index);
			final int startLineNumber = (int)(((index) / size) * (float)totalLineNumber);
			final int endLineNumber = (int)(((index+1.0) / size) * (float)totalLineNumber);
			Thread dataFileReader = new Thread(new Runnable() {	
				@Override
				public void run() {
					readDataFile(IPAddress, startLineNumber, endLineNumber);					
				}
			});
			dataFileReader.start();
			index--;
			if (index < 0) {
				index += 4.0;
			}
		}
	}
	
	private void readDataFile(String IPAddress, int startLineNumber, int endLineNumber) {
		String isLocal = localStorageIPs.contains(IPAddress) ? "local" : "replica";
		System.out.println("Started building the " +isLocal+" storage for IP: "+IPAddress+" | startLine: "+startLineNumber+ " endLine: " +endLineNumber);
		HashMap<String, ArrayList<String>> innerStorage = new HashMap<String, ArrayList<String>>();
		storage.put(IPAddress, innerStorage);
	    Scanner scanner = null;
		try {
			scanner = new Scanner(new FileInputStream(dataFileName), "UTF-8");
            int currentLineNumber = 0;
		    while (scanner.hasNextLine() && currentLineNumber <= endLineNumber){
		    	if (currentLineNumber <= startLineNumber) {
		    		scanner.nextLine();
		    		currentLineNumber++;
		    		continue;
		    	}
		    	String movie = scanner.nextLine();
                movie = movie.replace("\"", "");
                String[] keys = movie.split(" ");
                for (int i=0; i<keys.length; i++) {
                	String key = keys[i];
                    if (innerStorage.containsKey(key)) {
                    	ArrayList<String> values = innerStorage.get(key);
                    	values.add(movie);
                    } else {
                    	ArrayList<String> values = new ArrayList<String>();
                    	values.add(movie);
                    	innerStorage.put(key, values);
                    }
                }
                currentLineNumber++;
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
	      scanner.close();
	    }
		System.out.println("Built the " +isLocal+" storage for IP: "+IPAddress);
	}
	
	public void insert(String key, String value) {
		synchronized (storage) {
			boolean inserted = false;
			Set<String> allStorageIPs = storage.keySet();
			for (String IPAddress : allStorageIPs) {
				if (storage.get(IPAddress).containsKey(key)) {
					storage.get(IPAddress).get(key).add(value);
					inserted = true;
				}
			}
			if (inserted) {
				return;
			}
			int smallestSize = Integer.MAX_VALUE;
			String IPAddressWithSmallestSize = null;
			for (String IPAddress : allStorageIPs) {
				if (storage.get(IPAddress).size() < smallestSize) {
					IPAddressWithSmallestSize = IPAddress;
				}
			}	
			if (storage.get(IPAddressWithSmallestSize).containsKey(key)) {
				storage.get(IPAddressWithSmallestSize).get(key).add(value);
			}
			else {
				ArrayList<String> values = new ArrayList<String>();
				values.add(value);
				storage.get(IPAddressWithSmallestSize).put(key, values);				
			}
		}
	}

	public ArrayList<String> lookup(String key) {
		Set<String> allStorageIPs = storage.keySet();
		return lookupImpl(key, allStorageIPs);
	}
	
	public ArrayList<String> lookupLocal(String key) {
		return lookupImpl(key, localStorageIPs);
	}
	
	private ArrayList<String> lookupImpl(String key, Set<String> scope) {
		ArrayList<String> values = new ArrayList<String>();
		for (String IPAddress : scope) {
			if (storage.get(IPAddress).containsKey(key)) {
				values.addAll(storage.get(IPAddress).get(key));
			}
		}
		if (values.size() == 0) {
			return null;
		}
		return values;
	}
	
	public void delete(String key) {
		for (String IPAddress : localStorageIPs) {
			synchronized (storage) {
				if (storage.get(IPAddress).containsKey(key)) {
					storage.get(IPAddress).remove(key);
				}
			}
		}	
	}
	
	public void changeIsLocalStorage(String IPAddress, boolean isLocalStorage) {
		if (isLocalStorage) {
			if (!localStorageIPs.contains(IPAddress)) {
				localStorageIPs.add(IPAddress);
			}
		}
		else {
			if (localStorageIPs.contains(IPAddress)) {
				localStorageIPs.remove(IPAddress);
			}
		}
	}
	
	private int getLineNumberOfFile() {
	    Scanner scanner = null;
		int count = 0;
		try {
			scanner = new Scanner(new FileInputStream(dataFileName), "UTF-8");
		    while (scanner.hasNextLine()){
		    	scanner.nextLine();
		    	count++;
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
	      scanner.close();
	    }
		return count;
	}
	
	public HashMap<String, ArrayList<String>> getLocalStorage() {
		return storage.get(ownIPAddress);
	}
	
	public HashSet<String> getLocalStorageIPs() {
		return localStorageIPs;
	}
	
	public int getLocalStorageSize() {
		int size = 0;
		for (String IPAddress : localStorageIPs) {
			size += storage.get(IPAddress).size();
		}
		return size;
	}
	
	public String getStorageSizesAsString() {
		StringBuffer stringBuffer = new StringBuffer();
		Set<String> allStorageIPs = storage.keySet();
		for (String IPAddress : allStorageIPs) {
			String isLocal = localStorageIPs.contains(IPAddress) ? "local" : "replica";
			stringBuffer.append("Inner Storage for "+IPAddress+" ("+isLocal+") = "+storage.get(IPAddress).size()+"\n");
		}
		return stringBuffer.toString();
	}
	
	public Set<String> getStorageKeyIPs() {
		return storage.keySet();
	}

	public void printStorage() {
		synchronized (storage) {
			System.out.println(storage.toString());
		}	
	}

}