import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
import java.util.Set;


public class Storage {
	
	private HashMap<String, HashMap<String, ArrayList<String>>> storage = new HashMap<String, HashMap<String, ArrayList<String>>>();
	
	private String dataFileName;
	
	private String ownIPAddress;
	
	public Storage(String dataFileName, ArrayList<String> systemIPList, String ownIPAddress) {
		this.dataFileName = dataFileName;
		this.ownIPAddress = ownIPAddress;
		buildStorage(systemIPList);
		System.out.println("Built the storage");
	}
	
	private void buildStorage(ArrayList<String> systemIPList) {
		int totalLineNumber = getLineNumberOfFile();
		float index = systemIPList.indexOf(ownIPAddress);
		float size = systemIPList.size();
		int startLineNumber = (int)(((index) / size) * (float)totalLineNumber);
		int endLineNumber = (int)(((index+1.0) / size) * (float)totalLineNumber);
		readDataFile(ownIPAddress, startLineNumber, endLineNumber);
	}
	
	private void readDataFile(String IPAddress, int startLineNumber, int endLineNumber) {
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
	}
	
	public void insert(String IPAddress, String key, String value) {
		synchronized (storage) {
			if (storage.containsKey(IPAddress)) {
				HashMap<String, ArrayList<String>> innerStorage = storage.get(IPAddress);
				if (innerStorage.containsKey(key)) {
					 ArrayList<String> values = innerStorage.get(key);
					 values.add(value);
				} 
				else {
					ArrayList<String> values = new ArrayList<String>();
					values.add(value);
					innerStorage.put(key, values);
				}
			}
			else {
				ArrayList<String> values = new ArrayList<String>();
				values.add(value);
				HashMap<String, ArrayList<String>> innerStorage = new HashMap<String, ArrayList<String>>();
				innerStorage.put(key, values);
				storage.put(IPAddress, innerStorage);
			}
		}
	}
	
	public void insertReplicaStorage(String IPAddress, HashMap<String, ArrayList<String>> replicaStorage) {
		HashMap<String, ArrayList<String>> innerStorage = new HashMap<String, ArrayList<String>>();
		Set<String> keys = replicaStorage.keySet();
		for (String key : keys) {
			ArrayList<String> values = replicaStorage.get(key);
			ArrayList<String> copyValues = new ArrayList<String>();
			for (String value : values) {
				copyValues.add(value);
			}
			innerStorage.put(key, copyValues);
		}
		storage.put(IPAddress, innerStorage);
 	}

	public ArrayList<String> lookup(String IPAddress, String key) {
		synchronized (storage) {
			if (!storage.containsKey(IPAddress)) {
				return null;
			}
			if (!storage.get(IPAddress).containsKey(key)) {
				return null;
			}
			return storage.get(IPAddress).get(key);
		}

	}
	
	public void delete(String IPAddress, String key) {
		synchronized (storage) {
			if (!storage.containsKey(IPAddress)) {
				return;
			}
			if (!storage.get(IPAddress).containsKey(key)) {
				return;
			}
			storage.get(IPAddress).remove(key);
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
	
	public int getSize() {
		synchronized (storage) {
			return storage.get(ownIPAddress).size();
		}
	}

	public void printStorage() {
		synchronized (storage) {
			System.out.println(storage.toString());
		}	
	}

}
