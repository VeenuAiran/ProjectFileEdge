package poke.server.routing;

import java.util.HashMap;
import java.util.List;

import eye.Comm.Document;
import eye.Comm.PayloadReply;
import eye.Comm.Request;
import eye.Comm.Response;
import eye.Comm.RoutingPath;
import eye.Comm.Header.ReplyStatus;
import poke.server.Server;
import poke.server.queue.PerChannelQueue;
import poke.server.resources.ResourceUtil;

/**
 * This is a utility class that will help mapping the correlation id with request between the nodes.
 * @author veenu
 *
 */
public class ReqRespCorrelationIdMappingUtil {

	private static ReqRespCorrelationIdMappingUtil reqReslCorrInstance;
	//Currently, we refer "tag" field in the Proto file header message.
	private static Integer correlationId = 0;
	
	//This HashMap will help in tracking the perChannelQueue associated with request and response between any two nodes.
	//It will use the CorrelationID as the key.
	public static HashMap<Integer, PerChannelQueue> correlationMapping = new HashMap<Integer, PerChannelQueue>();	
	
	/**
	 * Private constructor.
	 */
	private ReqRespCorrelationIdMappingUtil(){
		
	}
	
	/**
	 * The method to fetch the instance of the mapping.
	 * @return
	 */
	public static synchronized ReqRespCorrelationIdMappingUtil getInstance(){
		
		if(null == reqReslCorrInstance){
			System.out.println("Creating a new instance of the ReqRespCorrelationIdMappingUtil");
			reqReslCorrInstance = new ReqRespCorrelationIdMappingUtil();
		}
		
		return reqReslCorrInstance;
	}
	
	/**
	 * The method to just maintain the correlation mapping.
	 * @param correlationId
	 * @param sq
	 */
	public void addCorrelationMapping(Integer correlationId, PerChannelQueue sq) {
		System.out.println("Inside the ReqRespCorrelationIdMapping.addCorrelationMapping()...");
		//Check if the correlation ID is already present in the mapping.
		if(!correlationMapping.containsKey(correlationId)){
			
			correlationMapping.put(correlationId, sq);
			System.out.println("Cached the PCQ...");
		}
		System.out.println("Exiting the ReqRespCorrelationIdMapping.addCorrelationMapping()...");
	}
	
	public static Integer getCorrelationId() {
		correlationId++;
		return correlationId;
	}
}
