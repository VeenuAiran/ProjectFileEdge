package poke.server.conf;

/**
 * This class has been added to store the info about any external node found for this server 
 * if there is any external node mentioned in the config file. 
 * @author krish
 *
 */
public class ExternalNodeInfo {

	//Class variables
	private String nodeId = "";
	private String host = "";
	private int port = 0;
	
	/**
	 * Public constructor.
	 * @param nodeId
	 * @param host
	 * @param port
	 */
	public ExternalNodeInfo(String nodeId, String host, int port){
		System.out.println("Inside the constructor for External Nodes");
		this.nodeId = nodeId;
		this.host = host;
		this.port = port;
	}
	
	//Getters and Setters.
	public String getNodeId() {
		return nodeId;
	}
	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	
	
}
