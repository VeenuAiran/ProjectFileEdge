/*
 * copyright 2013, gash
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
package poke.server.routing;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.GeneratedMessage;

import poke.client.ClientConnection;
import poke.client.ClientDecoderPipeline;
import poke.client.ClientHandler;
import poke.server.WhenServerBecomesClient;
import poke.server.conf.NodeDesc;
import poke.server.conf.ServerConf;
import poke.server.queue.PerChannelQueue;
import poke.server.resources.Resource;
import poke.server.resources.ResourceUtil;
import eye.Comm.Finger;
import eye.Comm.PayloadReply;
import eye.Comm.Request;
import eye.Comm.Response;
import eye.Comm.RoutingPath;

/**
 * The forward resource is used by the ResourceFactory to send requests to a
 * destination that is not this server.
 * 
 * Strategies used by the Forward can include TTL (max hops), durable tracking,
 * endpoint hiding.
 * 
 * @author gash
 * 
 */
public class ForwardResource implements Resource {
	protected static Logger logger = LoggerFactory.getLogger("server");
	
	//Trying to solve the issue. Having the code of WhenServerBecomesClient in ForwardResource.
	private String host;
	private int port;
	private ChannelFuture channelFuture; // do not use directly call connect()!
	private ClientBootstrap bootstrap;
	WhenServerBecomesClientDecoderPipeline serverBecomesClientPipeline;
	private LinkedBlockingDeque<com.google.protobuf.GeneratedMessage> outbound;
	private OutboundWorker worker;
	
	/**
	 * Public zero constructor.
	 */
	public ForwardResource(){
		
	}
	
	/**
	 * The constructor.
	 * @param host
	 * @param port
	 */
	protected ForwardResource(String host, int port) {
		this.host = host;
		this.port = port;

		init();
	}
	
	/**
	 * release all resources
	 */
	public void release() {
		bootstrap.releaseExternalResources();
	}
	
	/**
	 * Method would be called to connect to the immediate next node i.e. the first adjacent node.
	 * @param host
	 * @param port
	 * @return
	 */
	public static ForwardResource initConnection(String host, int port) {

		System.out.println("Inside the ForwardResource.initConnection()...");
		ForwardResource rtn = new ForwardResource(host, port);
		System.out.println("Exiting the ForwardResource.initConnection()...");
		return rtn;
	}
	
	/**
	 * The method would be called to initiate a connection through bootstrap.
	 * Similar to ClientConnection, but this time it would be to solve when Server becomes client.
	 */
	private void init() {
		System.out.println("Inside the ForwardResource.init()...");
		outbound = new LinkedBlockingDeque<com.google.protobuf.GeneratedMessage>();
		// Configure the client.
		bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool()));

		bootstrap.setOption("connectTimeoutMillis", 10000);
		bootstrap.setOption("tcpNoDelay", true);
		bootstrap.setOption("keepAlive", true);

		// Set up the pipeline factory.
		bootstrap.setPipelineFactory(new WhenServerBecomesClientDecoderPipeline());
		worker = new OutboundWorker(this);
		worker.start();
		System.out.println("Exiting the ForwardResource.init()...");
	}
	
	/**
	 * create connection to remote server
	 * 
	 * @return
	 */
	public Channel connect() { 
		System.out.println("Inside the ForwardResource.connect()...");
		// Start the connection attempt.
		if (channelFuture == null) {
			logger.info("---> connecting to:::" + port + " from ServerBecomesClient");
			channelFuture = bootstrap
					.connect(new InetSocketAddress(host, port));
			// cleanup on lost connection
		}

		// wait for the connection to establish
		channelFuture.awaitUninterruptibly();
		if (channelFuture.isDone() && channelFuture.isSuccess()) {

			System.out.println("Exiting the ForwardResource.connect()...");
			return channelFuture.getChannel();
		} else
			throw new RuntimeException(
					"Not able to establish connection to server");
	}
	//Trying - End

	private ServerConf cfg;

	public ServerConf getCfg() {
		return cfg;
	}

	/**
	 * Set the server configuration information used to initialized the server.
	 * 
	 * @param cfg
	 */
	public void setCfg(ServerConf cfg) {
		this.cfg = cfg;
	}
	
	/**
	 * In this method we just build a forward message and add the Server.ownNodeId i.e. the local servers node id
	 * in the routing path. After that we call the class WhenServerBecomesClient to act in a way that our server
	 * is not client. It will then be used to put the fwdReq to process. By that time, we return null as response.
	 * When the forwarded request gets processed that would be handled in WhenServerBecomesClientHandler.
	 */
	@Override
	public Response process(Request request) {
		//String nextNode = determineForwardNode(request);
		//if (nextNode != null) {
			
		System.out.println("Inside the ForwardResource.process()...");
		
			Request fwd = ResourceUtil.buildForwardMessage(request, null);
			
			try{
				System.out.println("Exiting the ForwardResource.process()...");
				if(fwd == null)
					System.out.println("Fwd request is null...");
				if(outbound == null)
					System.out.println("Is outbound null? :: ");
				outbound.put(fwd);
			}catch(InterruptedException ex){
				System.out.println("Some error :: "+ex.getMessage());
			}
			
			
			return null;
	}

	/**
	 * Find the nearest node that has not received the request.
	 * 
	 * TODO this should use the heartbeat to determine which node is active in
	 * its list.
	 * 
	 * @param request
	 * @return
	 */
	private String determineForwardNode(Request request) {
		List<RoutingPath> paths = request.getHeader().getPathList();
		if (paths == null || paths.size() == 0) {
			// pick first nearest
			NodeDesc nd = cfg.getNearest().getNearestNodes().values().iterator().next();
			return nd.getNodeId();
		} else {
			// if this server has already seen this message return null
			for (RoutingPath rp : paths) {
				for (NodeDesc nd : cfg.getNearest().getNearestNodes().values()) {
					if (!nd.getNodeId().equalsIgnoreCase(rp.getNode()))
						return nd.getNodeId();
				}
			}
		}

		return null;
	}
	
	/**
	 * queues outgoing messages - this provides surge protection if the client
	 * creates large numbers of messages.
	 * Same inner class as in Client Connection but this time it will use WhenServerBecomesClient class
	 * and its connections.
	 * 
	 * @author gash
	 * 
	 */
	protected class OutboundWorker extends Thread {
		ForwardResource conn;
		boolean forever = true;

		public OutboundWorker(ForwardResource conn) {
			this.conn = conn;

			if (conn.outbound == null)
				throw new RuntimeException("connection worker detected null queue");
		}

		@Override
		public void run() {
			System.out.println("Inside the ForwardResource.OutBoundWorker.run()...");
			Channel ch = conn.connect();
			if (ch == null || !ch.isOpen()) {
				ForwardResource.logger.error("connection missing, no outbound communication");
				return;
			}

			while (true) {
				if (!forever && conn.outbound.size() == 0)
					break;

				try {
					// block until a message is enqueued
					GeneratedMessage msg = conn.outbound.take();
					if (ch.isWritable()) {
						WhenServerBecomesClientHandler handler = conn.connect().getPipeline().get(WhenServerBecomesClientHandler.class);

						if (!handler.send(msg))
							conn.outbound.putFirst(msg);

					} else
						conn.outbound.putFirst(msg);
				} catch (InterruptedException ie) {
					break;
				} catch (Exception e) {
					ForwardResource.logger.error("Unexpected communcation failure", e);
					break;
				}
			}

			if (!forever) {
				ForwardResource.logger.info("connection queue closing");
			}
			
			System.out.println("Exiting the ForwardResource.OutBoundWorker.run()...");
		}
	}
}
