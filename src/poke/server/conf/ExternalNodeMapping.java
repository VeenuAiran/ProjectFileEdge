package poke.server.conf;

import java.util.HashMap;

/**
 * This class is added for mapping the external nodes.
 * @author krish
 *
 */
public class ExternalNodeMapping {

	//Map to map the external node info with IP and Port.
	public static HashMap<String, ExternalNodeInfo> externalNodeMap = new HashMap<String, ExternalNodeInfo>();
	
	/**
	 * The public constructor.
	 */
	public ExternalNodeMapping(){
		
	}
	
	public void addExternalNodeInfoToMap(ExternalNodeInfo externalNode){
		if(!externalNodeMap.containsKey(externalNode.getHost())){
			externalNodeMap.put(externalNode.getNodeId(), externalNode);
			System.out.println("The external node added :: "+externalNode.getNodeId());
		}
	}

	public HashMap<String, ExternalNodeInfo> getExternalNodeMap() {
		return externalNodeMap;
	}

	public void setExternalNodeMap(HashMap<String, ExternalNodeInfo> externalNodeMap) {
		this.externalNodeMap = externalNodeMap;
	}
	
	
}
