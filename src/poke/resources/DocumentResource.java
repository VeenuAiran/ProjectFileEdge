/*
 * copyright 2012, gash
 * 
 * Gash licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package poke.resources;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import org.apache.commons.io.IOUtils;

import poke.client.ClientConnection;
import poke.client.util.ReadFileToByte;
import poke.constants.PokeNettyConstants;
import poke.server.Server;
import poke.server.conf.ExternalNodeMapping;
import poke.server.conf.NodeDesc;
import poke.server.conf.ServerConf;
import poke.server.resources.Resource;
import poke.server.resources.ResourceUtil;
import poke.server.routing.ForwardResource;
import poke.server.storage.jdbc.DatabaseStorage;
import poke.util.UtilMethods;
import eye.Comm.Document;
import eye.Comm.Finger;
import eye.Comm.Header;
import eye.Comm.NameSpace;
import eye.Comm.PayloadReply;
import eye.Comm.Request;
import eye.Comm.Response;
import eye.Comm.Header.ReplyStatus;
import eye.Comm.RoutingPath;

public class DocumentResource implements Resource {

	//Added for integration.
	protected static Logger logger = LoggerFactory.getLogger(DocumentResource.class);
	private DatabaseStorage databaseStorage = new DatabaseStorage();
	private List<DocAddBean> filesToTransfer = new ArrayList<DocAddBean>();
	public static HashMap<String, ForwardResource> fwdResourceMap = new HashMap<String, ForwardResource>();
	public static String nextExtNode = "";
	ForwardResource fwdResource = null;
	
	//Initialize the class variables.
	ServerConf cfg = null;
	ForwardResource fwdRsc = null;
	String path = "";
	
	//For my testing.
	String adjNodeHost = "";
	int adjNodePort = 0;
	
	// start thread that moves the files add to the list to neighbor node
	Thread th1 = new Thread() 
	{
		public void run() 
		{
			try 
			{
				logger.info("Starting thread that handles replication.......");
				Thread.sleep(2000); // sleep for 2 milliseconds
				synchronized (filesToTransfer) 
				{
					List<DocAddBean> toRemove = new ArrayList<DocAddBean>();
					for(DocAddBean docAddBean : filesToTransfer) 
					{
						logger.info("Processing .... "+docAddBean.toString());
						if(docAddBean.isIsowner()) 
						{
							if(databaseStorage.addDoc(docAddBean.getFileid(), docAddBean.getFilename(), docAddBean.getFilepath(), 
									docAddBean.getNamespace(), docAddBean.isIsowner(), false)) 
							{
								logger.info("Saved the file sent...now replicate to neighbor.");
								boolean replicationFailed = false;
								List<String> keysToSetConnAsNull = new ArrayList<String>();
								// Replicate to neighbor nodes since we are the owner of file
								for (Entry<String, ClientConnection> entry : Server.neighborToCcMap.entrySet())
								{
									logger.info("Replicating file to "+entry.getKey());
									ClientConnection conn = entry.getValue();
									if(conn != null)
									{
										if(conn.isConnected())
										{
											UtilMethods.transferFile(conn, docAddBean.getFilepath(), 
													docAddBean.getNamespace(), "SERVER");
											// insert into the replication table
											databaseStorage.addReplicatedDoc(docAddBean.getFileid(), entry.getKey());
										}
										else
										{
											keysToSetConnAsNull.add(entry.getKey());
											replicationFailed = true;
										}
									}
									else
									{
										replicationFailed = true;
									}
								}
								
								for(String key : keysToSetConnAsNull)
								{
									Server.neighborToCcMap.put(key, null);
								}
								// Update the replication status for this file in database after 
								//  trying to replicate in neighboring nodes
								databaseStorage.updateReplicationStatus(docAddBean.getFileid(), !replicationFailed);
								// Since the transfer is done now remove the entry from list.
								toRemove.add(docAddBean);
							}
						}
						// Replication request
						else 
						{
							if(databaseStorage.addDoc(docAddBean.getFileid(), docAddBean.getFilename(), docAddBean.getFilepath(), 
									docAddBean.getNamespace(), docAddBean.isIsowner(), false)) 
							{
								logger.info("Saved the file sent for replication purposes.");
								// Since the transfer is done now remove the entry from list.
								toRemove.add(docAddBean);
							}
						}
					}
					filesToTransfer.removeAll(toRemove);
				}
			}
			catch (InterruptedException e) 
			{
				e.printStackTrace();
			}
			logger.info("One cycle over thread that handles replication.......");
		}
	};

	/**
	 * The message will create a response. If the request is a handshake request for searching the file, then response
	 * will not have any document in PayLoad Reply. We just send the header.
	 * Based on the type of request (detected using routing id) we process the request appropriately.
	 */
	@Override
	public Response process(Request request) {
		// TODO Auto-generated method stub
		System.out.println("Inside the poke.resource.DocumentResource.java class...");
		
		System.out.println("Processing the request "+request.toString());
		String lResponseMsg = "";
		String nodeInfo = "";
		byte[] bytesRead = null;
		ReadFileToByte rftb = new ReadFileToByte();
		
		//Added the logic to stop the resource from servicing a request that traveled a loop with the cluster.
		//For Eg. client-->A-->B-->C-->D-->A... In this case A would receive request from D that already went from A.
		//So we will check the routing path to see if A is their in path.
		if(verifyIfRequestTraveledALoop(request)){
			return buildReqResWhenTravelledLoop(request);
		}		
		
		if(request != null){
			System.out.println("The sent request is not null...");
			
			if(request.getHeader().getRoutingId() == eye.Comm.Header.Routing.DOCFIND){
				System.out.println("The request if for document find...");
				
				eye.Comm.Header lHeader = request.getHeader();
				
				eye.Comm.Payload lPayload = request.getBody();
				eye.Comm.NameSpace lNameSpace = lPayload.getSpace();
				eye.Comm.Document lDocument = lPayload.getDoc();
				path = searchForDocument(lNameSpace.getName(), lDocument.getDocName());
				
				if(path != "" || !path.equals("")){
					System.out.println("path where the document exists :: "+path);
					lResponseMsg = "Requested File Found";
					System.out.println("Calling the util.ReadFileToByte class...");
					bytesRead = rftb.convertFileToByte(new File(path));
					
					//Start building the response.
					Response.Builder lResponse = Response.newBuilder();
					
					//Set the payload body for the response.
					PayloadReply.Builder pr = PayloadReply.newBuilder();
					
					//Build the doc message to be set in reply.
					Document.Builder doc = Document.newBuilder();
					doc.setDocName(request.getBody().getDoc().getDocName());
					doc.setChunkContent(ByteString.copyFrom(bytesRead));
					doc.setChunkId(1);
					doc.setTotalChunk(1);
					pr.addDocs(doc.build());
					
					//Set the header for the response.
					//lResponse.setHeader(ResourceUtil.buildHeaderFrom(request.getHeader(), ReplyStatus.SUCCESS, lResponseMsg));
					
					Header.Builder respHeaderBuilder = Header.newBuilder(request.getHeader());
					
					respHeaderBuilder.setReplyCode(ReplyStatus.SUCCESS);
					respHeaderBuilder.setReplyMsg(lResponseMsg);
					
					lResponse.setHeader(respHeaderBuilder.build());
					//Set the body in response.
					lResponse.setBody(pr.build());
					
					Response reply = lResponse.build();
					
					System.out.println("Exiting the DocumentResource.process()...");
					
					return reply;
				}else{
					
					System.out.println("The file could not be found on the local server. Forwarding the request...");
					//Start forwarding the request to the Forward Resource.
					/*if(Server.ownNodeId == "zero" || Server.ownNodeId.equals("zero")){
						adjNodeHost = "localhost";
						adjNodePort = 5571;
					}else if(Server.ownNodeId == "one" || Server.ownNodeId.equals("one")){
						adjNodeHost = "localhost";
						adjNodePort = 5572;
					}else if(Server.ownNodeId == "two" || Server.ownNodeId.equals("two")){
						adjNodeHost = "localhost";
						adjNodePort = 5573;
					}else if(Server.ownNodeId == "three" || Server.ownNodeId.equals("three")){
						adjNodeHost = "localhost";
						adjNodePort = 5570;
					}*/
					if(fwdResourceMap.containsKey(PokeNettyConstants.MY_ADJACENT_NODE_HOST)){
						fwdResource = fwdResourceMap.get(PokeNettyConstants.MY_ADJACENT_NODE_HOST);
					}else{
						fwdResource = ForwardResource.initConnection(PokeNettyConstants.MY_ADJACENT_NODE_HOST, 
								Integer.parseInt(PokeNettyConstants.MY_ADJACENT_NODE_PORT));
						fwdResourceMap.put(PokeNettyConstants.MY_ADJACENT_NODE_HOST, fwdResource);
					}
					/*ForwardResource fwdResource = ForwardResource.initConnection(adjNodeHost, adjNodePort);*/
					
					System.out.println("Exiting the DocumentResource.process()...");
					return fwdResource.process(request);
				}
				//Added for Integration.
			}else if(request.getHeader().getRoutingId() == eye.Comm.Header.Routing.DOCADD){
				PayloadReply.Builder pb = PayloadReply.newBuilder();
				Finger.Builder fb = Finger.newBuilder();
				
				// Get Namespace from request
				NameSpace namespace = request.getBody().getSpace();
				NameSpace.Builder nb = null;
				if(namespace != null){
					nb = NameSpace.newBuilder();
					nb.setName(namespace.getName());
				}
				pb.addSpaces(nb.build());
				
				Document doc = request.getBody().getDoc();
				eye.Comm.Document.Builder d = null;
				ReplyStatus replyStatus = ReplyStatus.SUCCESS;
				
				if(doc != null)
				{
					String fileName = doc.getDocName();
					ByteString fileContent = doc.getChunkContent();
					if(fileName != null && fileContent != null && fileName.length() > 0 && !fileContent.isEmpty())
					{
						logger.info("Coying file : "+fileName);
						
						replyStatus = docAdd(fileName, fileContent, namespace, 
								request.getHeader().getOriginator());
					}
					
					d = eye.Comm.Document.newBuilder();
					d.setDocName(fileName);
					pb.addDocs(d.build());
				}
				
				Response.Builder rb = Response.newBuilder();

				// metadata
				rb.setHeader(ResourceUtil.buildHeaderFrom(request.getHeader(), replyStatus, null));
				
				fb.setTag(request.getBody().getFinger().getTag());
				fb.setNumber(request.getBody().getFinger().getNumber());
				pb.setFinger(fb.build());
				rb.setBody(pb.build());

				Response reply = rb.build();
				
				return reply;
			}else if(request.getHeader().getRoutingId() == eye.Comm.Header.Routing.DOCQUERY){
				System.out.println("The request is for document query...");
				
				eye.Comm.Header lHeader = request.getHeader();
				
				eye.Comm.Payload lPayload = request.getBody();
				eye.Comm.NameSpace lNameSpace = lPayload.getSpace();
				eye.Comm.Document lDocument = lPayload.getDoc();
				
				long remainingHopCount = lHeader.getRemainingHopCount();
				//We do need to process the request for doc query if the remaining hop count is 0 or more.
				if(remainingHopCount >= 0){
					path = searchForDocument(lNameSpace.getName(), lDocument.getDocName());
					//If I have the document then I will return the path to you. So please check for the path.
					if(path != "" || !path.equals("")){
						System.out.println("path where the document exists :: "+path);
						lResponseMsg = "Requested File Found";
						
						//As I have the document. Let me build a response and send it back to the node or client who asked
						//for this service.
						//Start building the response.
						Response.Builder lResponse = Response.newBuilder();
						
						//Set the payload body for the response.
						PayloadReply.Builder pr = PayloadReply.newBuilder();
						
						//Build the doc message to be set in reply.
						//I will only send the document name to the node or client along with a response message.
						Document.Builder doc = Document.newBuilder();
						doc.setDocName(request.getBody().getDoc().getDocName());
						pr.addDocs(doc.build());
						
						//Set the header for the response.
						//lResponse.setHeader(ResourceUtil.buildHeaderFrom(request.getHeader(), ReplyStatus.SUCCESS, lResponseMsg));
						
						Header.Builder respHeaderBuilder = Header.newBuilder(request.getHeader());
						
						respHeaderBuilder.setReplyCode(ReplyStatus.SUCCESS);
						respHeaderBuilder.setReplyMsg(lResponseMsg);
						
						lResponse.setHeader(respHeaderBuilder.build());
						
						//Set the body in response.
						lResponse.setBody(pr.build());
						
						Response reply = lResponse.build();
						
						System.out.println("Exiting the DocumentResource.process()...");
						
						return reply;
					}else{
						//Wait... I do not seem to have this requested document. So I will help by forwarding the request
						//to my next node. But sure... I do not want this request to travel a whole lot of my cluster.
						//Let me decrement the remaining hop count and then forward the request.
						//remainingHopCount--;
						System.out.println("Remaining hops left for this request :: "+remainingHopCount);
						System.out.println("The file could not be found on the local server. Forwarding the request...");
						
						Request.Builder bldr = Request.newBuilder(request);
						Header.Builder hbldr = bldr.getHeaderBuilder();
						
						//hbldr.setRemainingHopCount(remainingHopCount);
						
						hbldr.build();
						
						/*//Start forwarding the request to the Forward Resource.
						if(Server.ownNodeId == "zero" || Server.ownNodeId.equals("zero")){
							adjNodeHost = "localhost";
							adjNodePort = 5571;
						}else if(Server.ownNodeId == "one" || Server.ownNodeId.equals("one")){
							adjNodeHost = "localhost";
							adjNodePort = 5572;
						}else if(Server.ownNodeId == "two" || Server.ownNodeId.equals("two")){
							adjNodeHost = "localhost";
							adjNodePort = 5573;
						}else if(Server.ownNodeId == "three" || Server.ownNodeId.equals("three")){
							adjNodeHost = "localhost";
							adjNodePort = 5570;
						}*/
						
						if(fwdResourceMap.containsKey(PokeNettyConstants.MY_ADJACENT_NODE_HOST)){
							fwdResource = fwdResourceMap.get(PokeNettyConstants.MY_ADJACENT_NODE_HOST);
						}else{
							fwdResource = ForwardResource.initConnection(PokeNettyConstants.MY_ADJACENT_NODE_HOST, 
									Integer.parseInt(PokeNettyConstants.MY_ADJACENT_NODE_PORT));
							fwdResourceMap.put(PokeNettyConstants.MY_ADJACENT_NODE_HOST, fwdResource);
						}
						/*ForwardResource fwdResource = ForwardResource.initConnection(adjNodeHost, adjNodePort);*/
						
						System.out.println("Exiting the DocumentResource.process()...");
						return fwdResource.process(bldr.build());
					}
				}
				return null;
			}else if(request.getHeader().getRoutingId() == eye.Comm.Header.Routing.DOCREMOVE){
				System.out.println("The request id for Document Remove...");
				String deleteMsg = "";
				//Here I need to remove the document from my database and local disk and then forward the 
				//request to my buddy nodes to have it deleted from there nodes too.
				eye.Comm.Header lHeader = request.getHeader();
				
				eye.Comm.Payload lPayload = request.getBody();
				eye.Comm.NameSpace lNameSpace = lPayload.getSpace();
				eye.Comm.Document lDocument = lPayload.getDoc();
				
				path = searchForDocument(lNameSpace.getName(), lDocument.getDocName());
				
				//But let me tell one thing... If i find it I will delete that document... and also forward to 
				//my buddy nodes. But I am going to do the same thing, even when I do not find the document.
				if(path != "" || !path.equals("")){
					//write the code to delete the doc and then forward.
					System.out.println("I have the document... Let me delete it...");
					deleteMsg = deleteDocFromNode(lNameSpace.getName(), lDocument.getDocName(), path);
					
					//If the deleteMsg is a good news I will let other nodes to delete in then also...
					if(deleteMsg == "Document deletion completed" || deleteMsg.equals("Document deletion completed")){
						//Forward the request to other nodes to delete the document.
						/*//Start forwarding the request to the Forward Resource.
						if(Server.ownNodeId == "zero" || Server.ownNodeId.equals("zero")){
							adjNodeHost = "localhost";
							adjNodePort = 5571;
						}else if(Server.ownNodeId == "one" || Server.ownNodeId.equals("one")){
							adjNodeHost = "localhost";
							adjNodePort = 5572;
						}else if(Server.ownNodeId == "two" || Server.ownNodeId.equals("two")){
							adjNodeHost = "localhost";
							adjNodePort = 5573;
						}else if(Server.ownNodeId == "three" || Server.ownNodeId.equals("three")){
							adjNodeHost = "localhost";
							adjNodePort = 5570;
						}*/
						
						if(fwdResourceMap.containsKey(PokeNettyConstants.MY_ADJACENT_NODE_HOST)){
							fwdResource = fwdResourceMap.get(PokeNettyConstants.MY_ADJACENT_NODE_HOST);
						}else{
							fwdResource = ForwardResource.initConnection(PokeNettyConstants.MY_ADJACENT_NODE_HOST, 
									Integer.parseInt(PokeNettyConstants.MY_ADJACENT_NODE_PORT));
							fwdResourceMap.put(PokeNettyConstants.MY_ADJACENT_NODE_HOST, fwdResource);
						}
						/*ForwardResource fwdResource = ForwardResource.initConnection(adjNodeHost, adjNodePort);*/
						
						System.out.println("Exiting the DocumentResource.process()...");
						return fwdResource.process(request);
					}
					
				}else{
					//Forward the request to buddy nodes to delete the doc.
					/*//Start forwarding the request to the Forward Resource.
					if(Server.ownNodeId == "zero" || Server.ownNodeId.equals("zero")){
						adjNodeHost = "localhost";
						adjNodePort = 5571;
					}else if(Server.ownNodeId == "one" || Server.ownNodeId.equals("one")){
						adjNodeHost = "localhost";
						adjNodePort = 5572;
					}else if(Server.ownNodeId == "two" || Server.ownNodeId.equals("two")){
						adjNodeHost = "localhost";
						adjNodePort = 5573;
					}else if(Server.ownNodeId == "three" || Server.ownNodeId.equals("three")){
						adjNodeHost = "localhost";
						adjNodePort = 5570;
					}*/
					
					if(fwdResourceMap.containsKey(PokeNettyConstants.MY_ADJACENT_NODE_HOST)){
						fwdResource = fwdResourceMap.get(PokeNettyConstants.MY_ADJACENT_NODE_HOST);
					}else{
						fwdResource = ForwardResource.initConnection(PokeNettyConstants.MY_ADJACENT_NODE_HOST, 
								Integer.parseInt(PokeNettyConstants.MY_ADJACENT_NODE_PORT));
						fwdResourceMap.put(PokeNettyConstants.MY_ADJACENT_NODE_HOST, fwdResource);
					}
					/*ForwardResource fwdResource = ForwardResource.initConnection(adjNodeHost, adjNodePort);*/
					
					System.out.println("Exiting the DocumentResource.process()...");
					return fwdResource.process(request);
				}
				return null;
			}
		}
		return null;
	}
	
	/**
	 * The method will search for the document if available.
	 * @param pNameSpace
	 * @param pFileName
	 */
	public String searchForDocument(String pNameSpace, String pFileName){
		System.out.println("Inside the DocumentResource.searchForDocument()...");
		
		String searchForDocQuery = PokeNettyConstants.SEARCH_FOR_DOC_QUERY;
		String filePath = databaseStorage.searchForDocument(searchForDocQuery, pNameSpace, pFileName);
		
		return filePath;
	}
	
	/**
	 * The method to delete the document from the database and from the local disk.
	 * @param pNameSpace
	 * @param pFileName
	 * @param filePath
	 * @return
	 */
	public String deleteDocFromNode(String pNameSpace, String pFileName, String filePath){
		System.out.println("Inside the DocumentResource.deleteDocFromNode()...");
		String deleteMsg = "";
		String deleteDocQuery = PokeNettyConstants.DELETE_DOC_QUERY;
		
		String msg = databaseStorage.deleteDocFromNode(pNameSpace, pFileName, deleteDocQuery);
		
		if(msg == "Success" || msg.equals("Success")){
			File toBeDeletedFile = new File(filePath);
			if(toBeDeletedFile.delete()){
				deleteMsg = "Document deletion completed";
				System.out.println("The specified file :: "+pFileName+" is deleted. :: "+deleteMsg);
			}else{
				System.out.println("The file :: "+pFileName+" has not been deleted.");
			}
		}
		System.out.println("Exiting the DocumentResource.deleteDocFromNode()...");
		return deleteMsg;
	}
	
	/**
	 * The method would verify if the request traveled a loop. If yes it will return true else false.
	 * @param request
	 * @return
	 */
	public boolean verifyIfRequestTraveledALoop(Request request){
		System.out.println("Inside the DocumentResource.verifyIfRequestTraveledALoop()...");
		boolean traveledLoop = false;
		List<RoutingPath> paths = null;
		
		if(request.getHeader().getPathList() == null){
			
			return false;
		}else{
			//Get the routing path from the request.
			//List<RoutingPath> paths = request.getHeader().getPathList();
			paths = request.getHeader().getPathList();
			if(paths != null && paths.size() != 0){
				System.out.println("Routing path has the entries...");
				System.out.println("Request came back again to me...");
				//Want to check if the Server's own node id is present in the routing path list.
				for(RoutingPath rp : paths){
					if(Server.ownNodeId.equals(rp.getNode())){
						System.out.println("Routing path list has the server's own node id. So this request traveled a loop...");
						traveledLoop = true;
					}
				}
			}
		}		
		System.out.println("Exiting the DocumentResource.verifyIfRequestTraveledALoop()...");
		return traveledLoop;
	}

	/**
	 * This method will build the response for the request.
	 * @param request
	 * @return
	 */
	public Response buildResponseWhenTravelledLoop(Request request){
		System.out.println("Inside the DocumentResource.buildResponseWhenTravelledLoop()...");
		//Start building the response.
		Response.Builder lResponse = Response.newBuilder();
		
		//Set the payload body for the response.
		PayloadReply.Builder pr = PayloadReply.newBuilder();
		
		//Build the doc message to be set in reply.
		Document.Builder doc = Document.newBuilder();
		doc.setDocName(request.getBody().getDoc().getDocName());
		pr.addDocs(doc.build());
		
		//Set the header for the response.
		if(request.getHeader().getRoutingId() == eye.Comm.Header.Routing.DOCREMOVE){
			lResponse.setHeader(ResourceUtil.buildHeaderFrom(request.getHeader(), ReplyStatus.SUCCESS, 
					"Spcefied file has been deleted from our cluster..."));
		}else{
			lResponse.setHeader(ResourceUtil.buildHeaderFrom(request.getHeader(), ReplyStatus.FAILURE, 
					"Requested File Not Found in the Cluster..."));
		}
		
		//Set the body in response.
		lResponse.setBody(pr.build());
		
		Response reply = lResponse.build();
		System.out.println("Exiting the DocumentResource.buildResponseWhenTravelledLoop()...");
		return reply;
	}
	
	/**
	 * This method is a modified version of earlier method buildResponseWhenTravelledLoop(req).
	 * Earlier, whenever a request traveled a loop within the cluster, we returned the response to the original client
	 * saying we do not have the document within our cluster. Now, this version of the method, will forward the request to the 
	 * external node.
	 * There are multiple check points. Currently we are forwarding only the docquery request to the external node, so when 
	 * the request is for DOCQUERY, we check, for presence of cycle in routing path and if yes, we add the isExternal = True
	 * and decrease the remaining hop count by 1 and again make the request travel the cluster.
	 * @param request
	 * @return
	 */
	public Response buildReqResWhenTravelledLoop(Request request){
		System.out.println("Inside the DocumentResource.buildReqResWhenTravelledLoop()...");
		ExternalNodeMapping extNodeMap = new ExternalNodeMapping();
		List<RoutingPath> paths = null;
		
		//Start building the response.
		if(request.getHeader().getRoutingId() == eye.Comm.Header.Routing.DOCREMOVE){
			Response.Builder lResponse = Response.newBuilder();
			
			//Set the payload body for the response.
			PayloadReply.Builder pr = PayloadReply.newBuilder();
			
			//Build the doc message to be set in reply.
			Document.Builder doc = Document.newBuilder();
			doc.setDocName(request.getBody().getDoc().getDocName());
			pr.addDocs(doc.build());
			
			//lResponse.setHeader(ResourceUtil.buildHeaderFrom(request.getHeader(), ReplyStatus.SUCCESS, 
				//	"Spcefied file has been deleted from our cluster..."));
			Header.Builder respHeaderBuilder = Header.newBuilder(request.getHeader());
			
			respHeaderBuilder.setReplyCode(ReplyStatus.SUCCESS);
			respHeaderBuilder.setReplyMsg("Spcefied file has been deleted from our cluster...");
			
			lResponse.setHeader(respHeaderBuilder.build());
			
			//Set the body in response.
			lResponse.setBody(pr.build());
			
			Response reply = lResponse.build();
			System.out.println("Exiting the DocumentResource.buildResponseWhenTravelledLoop()...");
			return reply;
		}else if(request.getHeader().getRoutingId() == eye.Comm.Header.Routing.DOCQUERY){
			Request.Builder bldr = Request.newBuilder(request);
			Header.Builder hbldr = bldr.getHeaderBuilder();
			
			long remainingHopCount = hbldr.getRemainingHopCount();
			
			//If the request has traveled a loop and also the remainingHopCount == 0, then we cannot forward to any other external
			//node but we need to sent failure response back saying the request cannot be satisfied at all.
			//So just build a response and send it back with some meaningful failure message.
			if(remainingHopCount == 0){
				Response.Builder lResponse = Response.newBuilder();
				
				//Set the payload body for the response.
				PayloadReply.Builder pr = PayloadReply.newBuilder();
				
				//Build the doc message to be set in reply.
				Document.Builder doc = Document.newBuilder();
				doc.setDocName(request.getBody().getDoc().getDocName());
				pr.addDocs(doc.build());
				
				Header.Builder respHeaderBuilder = Header.newBuilder(request.getHeader());
				
				respHeaderBuilder.setReplyCode(ReplyStatus.FAILURE);
				respHeaderBuilder.setReplyMsg("Spcefied file was not found in the cluster...");
				
				lResponse.setHeader(respHeaderBuilder.build());
				
				//Set the body in response.
				lResponse.setBody(pr.build());
				
				Response reply = lResponse.build();
				System.out.println("Exiting the DocumentResource.buildResponseWhenTravelledLoop()...");
				return reply;
			}
			
			if(extNodeMap.getExternalNodeMap().size() != 0){
				hbldr.setRemainingHopCount(remainingHopCount--);
				hbldr.setIsExternal(false);
				
				
				
				paths = request.getHeader().getPathList();
				
				Iterator it = extNodeMap.getExternalNodeMap().entrySet().iterator();
				while(it.hasNext()){
					Map.Entry pairs = (Map.Entry)it.next();
					if(paths != null && paths.size() != 0){
						for(RoutingPath rp : paths){
							if(!((String)pairs.getKey()).equals(rp.getNode())){
								System.out.println("Routing path list has the server's own node id. So this request traveled a loop...");
								nextExtNode = (String)pairs.getKey();
							}else{
								nextExtNode = "";
							}
						}
					}
					
					if(nextExtNode != "" || nextExtNode.equals("")){
						break;
					}
				}
				
				/*RoutingPath.Builder rpb = RoutingPath.newBuilder();
				rpb.setNode(nextExtNode);
				rpb.setTime(System.currentTimeMillis());
				
				hbldr.addPath(rpb.build());*/
				
				hbldr.build();
				
				if(fwdResourceMap.containsKey(extNodeMap.getExternalNodeMap().get(nextExtNode).getHost())){
					fwdResource = fwdResourceMap.get(extNodeMap.getExternalNodeMap().get(nextExtNode).getHost());
				}else{
					fwdResource = ForwardResource.initConnection(extNodeMap.getExternalNodeMap().get(nextExtNode).getHost(), 
							extNodeMap.getExternalNodeMap().get(nextExtNode).getPort());
					fwdResourceMap.put(extNodeMap.getExternalNodeMap().get(nextExtNode).getHost(), fwdResource);
				}
				/*ForwardResource fwdResource = ForwardResource.initConnection(adjNodeHost, adjNodePort);*/
				
				System.out.println("Exiting the buildResponseWhenTravelledLoop...");
				return fwdResource.process(bldr.build());
			}else{
				hbldr.setIsExternal(true);
				
				hbldr.build();
				
				if(fwdResourceMap.containsKey(PokeNettyConstants.MY_ADJACENT_NODE_HOST)){
					fwdResource = fwdResourceMap.get(PokeNettyConstants.MY_ADJACENT_NODE_HOST);
				}else{
					fwdResource = ForwardResource.initConnection(PokeNettyConstants.MY_ADJACENT_NODE_HOST, 
							Integer.parseInt(PokeNettyConstants.MY_ADJACENT_NODE_PORT));
					fwdResourceMap.put(PokeNettyConstants.MY_ADJACENT_NODE_HOST, fwdResource);
				}
				/*ForwardResource fwdResource = ForwardResource.initConnection(adjNodeHost, adjNodePort);*/
				
				System.out.println("Exiting the buildResponseWhenTravelledLoop...");
				return fwdResource.process(bldr.build());
			}
		}		
		return null;
	}
	
	/**
	 * This method would perform the work to add the document into the system. Since, we are replicating the document across the
	 * adjacent nodes. We do need to distinguish what type of save operation we need to do. If the request.getOriginator() == "client"
	 * we do upload the document and send the doc to adjacent node for replication. But when the adjacent nodes receive the request
	 * they would find request.getOriginator() value as sender server's id. So the nodes just save it on them and do not replicate further.
	 * We threaded processing as initially we were blocking the server to send the response to client till the time replication is complete.
	 * Now as soon as owner node uploads file to itself it send the response to client and parallaly replication is carried on.
	 * @param fileName
	 * @param fileContent
	 * @param namespace
	 * @param originator
	 * @return
	 */
	public ReplyStatus docAdd(String fileName, ByteString fileContent, eye.Comm.NameSpace namespace, String originator){
		try {
			String directory = PokeNettyConstants.COMMON_LOCATION+Server.SERVER_NAME+"/"+PokeNettyConstants.DEFAULT_NAMESPACE;
			String namespaceString = PokeNettyConstants.DEFAULT_NAMESPACE;
			
			if(namespace != null && !namespace.getName().isEmpty())
			{
				directory+="/"+namespace.getName();
				namespaceString = namespace.getName();
			}
			
			if(!new File(directory).exists())
			{
				logger.info("Directory does not exists ...creating directory : "+directory);
				new File(directory).mkdirs();
			}
			
			String filePath = directory+"/"+fileName;
			if(new File(filePath).exists())
			{
				logger.info("File already exists ...so not copying. location : "+filePath);
				return ReplyStatus.SUCCESS;
			}
			else
			{
				// save original file to local disk
				DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(new File(filePath)));
				IOUtils.write(fileContent.toByteArray(), dataOutputStream);
				IOUtils.closeQuietly(dataOutputStream);

				logger.info("Copied transfered file ...fileName : "+fileName+", originator : "+originator);
				
				// new file id
				Long fileId = System.nanoTime();
				
				// Add the Doc Add to list so that the replication thread process it and
				// complete the replication.
				synchronized (filesToTransfer) {
					filesToTransfer.add(new DocAddBean(fileId, fileName, filePath, namespaceString, 
							originator.equalsIgnoreCase("CLIENT")));
				}
				
				th1.start();

				return ReplyStatus.SUCCESS;
			}
		} 
		catch ( IOException e) 
		{
			System.err.println("An error occurred while copying the transferred file !!!");
			e.printStackTrace();
		} 
		
		return ReplyStatus.FAILURE;
	}
}
