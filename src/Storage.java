import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;


// TODO: Auto-generated Javadoc
/**
 * The Class Storage.
 */
public class Storage {
	
	/** The storage. */
	private HashMap<String, HashMap<String, ArrayList<String>>> storage = new HashMap<String, HashMap<String, ArrayList<String>>>();
	
	/** The local storage i ps. */
	private HashSet<String> localStorageIPs = new HashSet<String>();
	
	/** The data file name. */
	private String dataFileName;
	
	/** The own ip address. */
	private String ownIPAddress;
	
	/** The concurrent failure number. */
	private int concurrentFailureNumber;
	
	/**
	 * Instantiates a new storage.
	 *
	 * @param dataFileName the data file name
	 * @param systemIPList the system ip list
	 * @param ownIPAddress the own ip address
	 * @param concurrentFailureNumber the concurrent failure number
	 */
	public Storage(String dataFileName, ArrayList<String> systemIPList, String ownIPAddress, int concurrentFailureNumber) {
		this.dataFileName = dataFileName;
		this.ownIPAddress = ownIPAddress;
		this.concurrentFailureNumber = concurrentFailureNumber;
		localStorageIPs.add(ownIPAddress);
		buildStorage(systemIPList);
	}
	
	/**
	 * Builds the storage.
	 *
	 * @param systemIPList the system ip list
	 */
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
				index += systemIPList.size();
			}
		}
	}
	
	/**
	 * Read data file.
	 *
	 * @param IPAddress the iP address
	 * @param startLineNumber the start line number
	 * @param endLineNumber the end line number
	 */
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
	
	/**
	 * Insert.
	 *
	 * @param key the key
	 * @param value the value
	 */
	public void insert(String key, String value) {
		synchronized (storage) {
			boolean overwrittenTheKey = false;
			Set<String> allStorageIPs = storage.keySet();
			for (String IPAddress : allStorageIPs) {
				if (storage.get(IPAddress).containsKey(key)) {
					storage.get(IPAddress).get(key).add(value);
					overwrittenTheKey = true;
				}
			}
			if (overwrittenTheKey) {
				return;
			}
			int smallestSize = Integer.MAX_VALUE;
			String IPAddressWithSmallestSize = null;
			for (String IPAddress : allStorageIPs) {
				if (storage.get(IPAddress).size() < smallestSize) {
					IPAddressWithSmallestSize = IPAddress;
					smallestSize = storage.get(IPAddress).size();
				}
			}	
			ArrayList<String> values = new ArrayList<String>();
			values.add(value);
			storage.get(IPAddressWithSmallestSize).put(key, values);				
		}
	}

	/**
	 * Lookup.
	 *
	 * @param key the key
	 * @return the array list
	 */
	public ArrayList<String> lookup(String key) {
		Set<String> allStorageIPs = storage.keySet();
		return lookupImpl(key, allStorageIPs);
	}
	
	/**
	 * Lookup local.
	 *
	 * @param key the key
	 * @return the array list
	 */
	public ArrayList<String> lookupLocal(String key) {
		return lookupImpl(key, localStorageIPs);
	}
	
	/**
	 * Lookup impl.
	 *
	 * @param key the key
	 * @param scope the scope
	 * @return the array list
	 */
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
	
	/**
	 * Delete.
	 *
	 * @param key the key
	 */
	public void delete(String key) {
		for (String IPAddress : localStorageIPs) {
			synchronized (storage) {
				if (storage.get(IPAddress).containsKey(key)) {
					storage.get(IPAddress).remove(key);
				}
			}
		}	
	}
	
	/**
	 * Change is local storage.
	 *
	 * @param IPAddress the iP address
	 * @param isLocalStorage the is local storage
	 */
	public void changeIsLocalStorage(String IPAddress, boolean isLocalStorage) {
		if (isLocalStorage) {
			if (!localStorageIPs.contains(IPAddress) && storage.keySet().contains(IPAddress)) {
				localStorageIPs.add(IPAddress);
			}
		}
		else {
			if (localStorageIPs.contains(IPAddress)) {
				localStorageIPs.remove(IPAddress);
			}
		}
	}
	
	/**
	 * Gets the line number of file.
	 *
	 * @return the line number of file
	 */
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
	
	/**
	 * Gets the local storage.
	 *
	 * @return the local storage
	 */
	public HashMap<String, ArrayList<String>> getLocalStorage() {
		return storage.get(ownIPAddress);
	}
	
	/**
	 * Gets the local storage i ps.
	 *
	 * @return the local storage i ps
	 */
	public HashSet<String> getLocalStorageIPs() {
		return localStorageIPs;
	}
	
	/**
	 * Gets the local storage size.
	 *
	 * @return the local storage size
	 */
	public int getLocalStorageSize() {
		int size = 0;
		for (String IPAddress : localStorageIPs) {
			size += storage.get(IPAddress).size();
		}
		return size;
	}
	
	/**
	 * Gets the storage sizes as string.
	 *
	 * @return the storage sizes as string
	 */
	public String getStorageSizesAsString() {
		StringBuffer stringBuffer = new StringBuffer();
		Set<String> allStorageIPs = storage.keySet();
		for (String IPAddress : allStorageIPs) {
			String isLocal = localStorageIPs.contains(IPAddress) ? "local" : "replica";
			stringBuffer.append("Inner Storage for "+IPAddress+" ("+isLocal+") = "+storage.get(IPAddress).size()+"\n");
		}
		return stringBuffer.toString();
	}
	
	/**
	 * Gets the storage key i ps.
	 *
	 * @return the storage key i ps
	 */
	public Set<String> getStorageKeyIPs() {
		return storage.keySet();
	}

	/**
	 * Prints the storage.
	 */
	public void printStorage() {
		synchronized (storage) {
			System.out.println(storage.toString());
		}	
	}

}