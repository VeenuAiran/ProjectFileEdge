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
package poke.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.routing.ReqRespCorrelationIdMappingUtil;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage;

import eye.Comm.Document;
import eye.Comm.Finger;
import eye.Comm.Header;
import eye.Comm.NameSpace;
import eye.Comm.Payload;
import eye.Comm.Request;

/**
 * provides an abstraction of the communication to the remote server.
 * 
 * @author gash
 * 
 */
public class ClientConnection {
	protected static Logger logger = LoggerFactory.getLogger("client");

	private String host;
	private int port;
	private ChannelFuture channel; // do not use directly call connect()!
	private ClientBootstrap bootstrap;
	ClientDecoderPipeline clientPipeline;
	private LinkedBlockingDeque<com.google.protobuf.GeneratedMessage> outbound;
	private OutboundWorker worker;
	int correlationId = 0;

	protected ClientConnection(String host, int port) {
		this.host = host;
		this.port = port;

		init();
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}
	
	/**
	 * release all resources
	 */
	public void release() {
		bootstrap.releaseExternalResources();
	}

	public static ClientConnection initConnection(String host, int port) {

		//Added for integration
		try {
			Socket client = new Socket(host, port);
			if(!client.isConnected()) {
				if(client != null) {
					client.close();
				}
				return null;
			}
			if(client != null) {
				client.close();
			}
		} 
		catch (UnknownHostException e) {
			logger.info("Not able to connect to host : "+host+", port : "+port);
			return null;
		} 
		catch (IOException e) {
			logger.info("Not able to connect to host : "+host+", port : "+port);
			return null;
		}
		
		ClientConnection rtn = new ClientConnection(host, port);
		return rtn;
	}

	/**
	 * add an application-level listener to receive messages from the server (as
	 * in replies to requests).
	 * 
	 * @param listener
	 */
	public void addListener(ClientListener listener) {
		try {
			if (clientPipeline != null)
				clientPipeline.addListener(listener);
		} catch (Exception e) {
			logger.error("failed to add listener", e);
		}
	}

	public void poke(String tag, int num) {
		// data to send
		Finger.Builder f = eye.Comm.Finger.newBuilder();
		f.setTag(tag);
		f.setNumber(num);

		// payload containing data
		Request.Builder r = Request.newBuilder();
		eye.Comm.Payload.Builder p = Payload.newBuilder();
		p.setFinger(f.build());
		r.setBody(p.build());

		// header with routing info
		eye.Comm.Header.Builder h = Header.newBuilder();
		h.setOriginator("client");
		h.setTag("test finger");
		h.setTime(System.currentTimeMillis());
		h.setRoutingId(eye.Comm.Header.Routing.FINGER);
		r.setHeader(h.build());

		eye.Comm.Request req = r.build();

		try {
			// enqueue message
			outbound.put(req);
		} catch (InterruptedException e) {
			logger.warn("Unable to deliver message, queuing");
		}
	}
	
	/**
	 * The method to send a file for saving and replication.
	 * @param fileName
	 * @param fileContent
	 * @param namespace
	 * @param originator
	 */
	public void sendFile(String fileName,ByteString fileContent, String namespace, String originator) {
		logger.info("Copying file : "+fileName+", originator : "+originator);
		
		// data to send
		Finger.Builder f = eye.Comm.Finger.newBuilder();
		f.setTag(fileName);
		f.setNumber(fileContent.size());

		//document to be sent
		eye.Comm.Document.Builder d = eye.Comm.Document.newBuilder();
		d.setDocName(fileName);
		d.setChunkContent(fileContent);
		
		// namespace to be sent
		eye.Comm.NameSpace.Builder nb = eye.Comm.NameSpace.newBuilder();
		nb.setName(namespace);
		
		// payload containing data
		Request.Builder r = Request.newBuilder();
		eye.Comm.Payload.Builder p = Payload.newBuilder();
		p.setFinger(f.build());
		p.setDoc(d.build());
		p.setSpace(nb.build());
		r.setBody(p.build());

		// header with routing info
		eye.Comm.Header.Builder h = Header.newBuilder();
		h.setOriginator(originator);
		h.setTag("test DOCADD");
		h.setTime(System.currentTimeMillis());
		h.setRoutingId(eye.Comm.Header.Routing.DOCADD);
		r.setHeader(h.build());

		eye.Comm.Request req = r.build();

		try {
			// enqueue message
			outbound.put(req);
		} catch (InterruptedException e) {
			logger.warn("Unable to deliver message, queuing");
		}
	}
	
	/**
	 * This method is added to fetch the file from the server. If the server has it, it will return the file.
	 * If not the request is forwarded.
	 */
	public void fetchFile(String pNameSpace, String pFileName){
		logger.info("Inside the ClientConnection.fetchFile()....");
		//Namespace to search for.
		NameSpace.Builder lNameSpace = eye.Comm.NameSpace.newBuilder();
		lNameSpace.setName(pNameSpace);
		
		//Document to fetch
		Document.Builder doc = eye.Comm.Document.newBuilder();
		doc.setDocName(pFileName);
		
		// payload for requesting the file.
		Request.Builder lRequest = Request.newBuilder();
		eye.Comm.Payload.Builder lPayload = Payload.newBuilder();
		lPayload.setSpace(lNameSpace.build());
		lPayload.setDoc(doc.build());
		lRequest.setBody(lPayload.build());
		
		// header with routing info
		eye.Comm.Header.Builder lHeader = Header.newBuilder();
		lHeader.setOriginator("client");
		
		//Added to set the correlationId and request id. Treating Tag = CorrelationId and time = requestId
		
		Random randomGenerator = new Random();
	    for (int idx = 1; idx <= 10; ++idx){
	      correlationId = randomGenerator.nextInt(100);
	      logger.info("Generated : " + correlationId);
	    }
		//int correlationId = ReqRespCorrelationIdMappingUtil.getInstance().getCorrelationId();
		lHeader.setCorrelationId(""+correlationId);
		lHeader.setTime(System.currentTimeMillis());
		//Add complete.
		
		lHeader.setRoutingId(eye.Comm.Header.Routing.DOCFIND);
		//lHeader.setFindHandShake("find");
		lRequest.setHeader(lHeader.build());
		
		//Building the request.
		eye.Comm.Request request = lRequest.build();
		
		try {
			logger.info("Enqueued the request on to the outbout queue");
			logger.info("Exiting the ClientConnection.fetchFile()...");
			// enqueue message
			outbound.put(request);
		} catch (InterruptedException e) {
			logger.warn("Unable to deliver message, queuing");
		}
		
	}
	
	/**
	 * This method is added to query for the file from the server. The request is built by adding the remainingHopCount.
	 * The first node that receives the request will use the remainingHopCount to gain the info about how far the request
	 * must travel within the cluster.
	 */
	public void queryForFile(String pNameSpace, String pFileName){
		logger.info("Inside the ClientConnection.queryForFile()....");
		//Namespace to search for.
		NameSpace.Builder lNameSpace = eye.Comm.NameSpace.newBuilder();
		lNameSpace.setName(pNameSpace);
		
		//Document to fetch
		Document.Builder doc = eye.Comm.Document.newBuilder();
		doc.setDocName(pFileName);
		
		// payload for requesting the file.
		Request.Builder lRequest = Request.newBuilder();
		eye.Comm.Payload.Builder lPayload = Payload.newBuilder();
		lPayload.setSpace(lNameSpace.build());
		lPayload.setDoc(doc.build());
		lRequest.setBody(lPayload.build());
		
		// header with routing info
		eye.Comm.Header.Builder lHeader = Header.newBuilder();
		lHeader.setOriginator("client");
		
		Random randomGenerator = new Random();
	    for (int idx = 1; idx <= 10; ++idx){
	      correlationId = randomGenerator.nextInt(100);
	      logger.info("Generated : " + correlationId);
	    }
		//int correlationId = ReqRespCorrelationIdMappingUtil.getInstance().getCorrelationId();
	    lHeader.setCorrelationId(""+correlationId);
		lHeader.setTime(System.currentTimeMillis());
		lHeader.setRemainingHopCount(1);
		//Add complete.
		
		lHeader.setRoutingId(eye.Comm.Header.Routing.DOCQUERY);
		//lHeader.setFindHandShake("find");
		lRequest.setHeader(lHeader.build());
		
		//Building the request.
		eye.Comm.Request request = lRequest.build();
		
		try {
			logger.info("Enqueued the request on to the outbout queue");
			logger.info("Exiting the ClientConnection.fetchFile()...");
			// enqueue message
			outbound.put(request);
		} catch (InterruptedException e) {
			logger.warn("Unable to deliver message, queuing");
		}
		
	}

	/**
	 * The method to delete the file from the cluster.
	 * @param pNameSpace
	 * @param pFileName
	 */
	public void removeFile(String pNameSpace, String pFileName){
		logger.info("Inside the ClientConnection.removeFile()....");
		//Namespace to search for.
		NameSpace.Builder lNameSpace = eye.Comm.NameSpace.newBuilder();
		lNameSpace.setName(pNameSpace);
		
		//Document to fetch
		Document.Builder doc = eye.Comm.Document.newBuilder();
		doc.setDocName(pFileName);
		
		// payload for requesting the file.
		Request.Builder lRequest = Request.newBuilder();
		eye.Comm.Payload.Builder lPayload = Payload.newBuilder();
		lPayload.setSpace(lNameSpace.build());
		lPayload.setDoc(doc.build());
		lRequest.setBody(lPayload.build());
		
		// header with routing info
		eye.Comm.Header.Builder lHeader = Header.newBuilder();
		lHeader.setOriginator("client");
		
		Random randomGenerator = new Random();
	    for (int idx = 1; idx <= 10; ++idx){
	      correlationId = randomGenerator.nextInt(100);
	      logger.info("Generated : " + correlationId);
	    }
		//int correlationId = ReqRespCorrelationIdMappingUtil.getInstance().getCorrelationId();
	    lHeader.setCorrelationId(""+correlationId);
		lHeader.setTime(System.currentTimeMillis());
		//Add complete.
		
		lHeader.setRoutingId(eye.Comm.Header.Routing.DOCREMOVE);
		//lHeader.setFindHandShake("find");
		lRequest.setHeader(lHeader.build());
		
		//Building the request.
		eye.Comm.Request request = lRequest.build();
		
		try {
			logger.info("Enqueued the request on to the outbout queue");
			logger.info("Exiting the ClientConnection.fetchFile()...");
			// enqueue message
			outbound.put(request);
		} catch (InterruptedException e) {
			logger.warn("Unable to deliver message, queuing");
		}
	}
	
	private void init() {
		// the queue to support client-side surging
		outbound = new LinkedBlockingDeque<com.google.protobuf.GeneratedMessage>();

		// Configure the client.
		bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool()));

		bootstrap.setOption("connectTimeoutMillis", 10000);
		bootstrap.setOption("tcpNoDelay", true);
		bootstrap.setOption("keepAlive", true);

		// Set up the pipeline factory.
		clientPipeline = new ClientDecoderPipeline();
		bootstrap.setPipelineFactory(clientPipeline);

		// start outbound message processor
		worker = new OutboundWorker(this);
		worker.start();
	}

	/**
	 * create connection to remote server
	 * 
	 * @return
	 */
	protected Channel connect() {
		// Start the connection attempt.
		if (channel == null) {
			// System.out.println("---> connecting");
			channel = bootstrap.connect(new InetSocketAddress(host, port));

			// cleanup on lost connection

		}

		// wait for the connection to establish
		channel.awaitUninterruptibly();

		if (channel.isDone() && channel.isSuccess())
			return channel.getChannel();
		else
			throw new RuntimeException("Not able to establish connection to server");
	}

	/**
	 * queues outgoing messages - this provides surge protection if the client
	 * creates large numbers of messages.
	 * 
	 * @author gash
	 * 
	 */
	protected class OutboundWorker extends Thread {
		ClientConnection conn;
		boolean forever = true;

		public OutboundWorker(ClientConnection conn) {
			this.conn = conn;

			if (conn.outbound == null)
				throw new RuntimeException("connection worker detected null queue");
		}

		@Override
		public void run() {
			Channel ch = conn.connect();
			if (ch == null || !ch.isOpen()) {
				ClientConnection.logger.error("connection missing, no outbound communication");
				return;
			}

			while (true) {
				if (!forever && conn.outbound.size() == 0)
					break;

				try {
					// block until a message is enqueued
					GeneratedMessage msg = conn.outbound.take();
					if (ch.isWritable()) {
						ClientHandler handler = conn.connect().getPipeline().get(ClientHandler.class);

						if (!handler.send(msg))
							conn.outbound.putFirst(msg);

					} else
						conn.outbound.putFirst(msg);
				} catch (InterruptedException ie) {
					break;
				} catch (Exception e) {
					ClientConnection.logger.error("Unexpected communcation failure", e);
					break;
				}
			}

			if (!forever) {
				ClientConnection.logger.info("connection queue closing");
			}
		}
	}
	
	@SuppressWarnings("unused")
	public boolean isConnected()
	{
		Socket client = null;
		try {
			client = new Socket(host, port);
			if(client != null) {
				return client.isConnected();
			}
		} 
		catch (UnknownHostException e) {
			logger.info("Not able to connect to host : "+host+", port : "+port);
			return false;
		} 
		catch (IOException e) {
			logger.info("Not able to connect to host : "+host+", port : "+port);
			return false;
		}
		finally {
			if(client != null) {
				try {
					client.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return false;
	}
}
