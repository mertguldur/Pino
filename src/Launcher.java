// TODO: Auto-generated Javadoc
/**
 * The Class Launcher.
 */
public class Launcher {

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {
		int port = Integer.parseInt(args[0]);
		String dataFileName = args[1];
		int concurrentFailureNumber = Integer.parseInt(args[2]);
		new Controller(port, dataFileName, concurrentFailureNumber);
	}

}
