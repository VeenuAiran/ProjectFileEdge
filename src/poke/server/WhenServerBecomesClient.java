package poke.server;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.GeneratedMessage;

import eye.Comm.Request;
import poke.client.ClientConnection;
import poke.client.ClientHandler;
import poke.server.routing.WhenServerBecomesClientDecoderPipeline;
import poke.server.routing.WhenServerBecomesClientHandler;

/**
 * This class will be used when a particular server has to interact with another server.
 * The behavior would be slightly similar to the ClientConnection class.
 * @author krish
 *
 */
public class WhenServerBecomesClient {

	public static Logger logger = LoggerFactory.getLogger("whenServerBecomesClient");

	private String host;
	private int port;
	private ChannelFuture channelFuture; // do not use directly call connect()!
	private ClientBootstrap bootstrap;
	private LinkedBlockingDeque<com.google.protobuf.GeneratedMessage> outbound;

	/**
	 * The constructor.
	 * @param host
	 * @param port
	 */
	protected WhenServerBecomesClient(String host, int port) {
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
	public static WhenServerBecomesClient initConnection(String host, int port) {

		System.out.println("Inside the WhenServerBecomesClient.initConnection()...");
		WhenServerBecomesClient rtn = new WhenServerBecomesClient(host, port);
		System.out.println("Exiting the WhenServerBecomesClient.initConnection()...");
		return rtn;
	}
	
	/**
	 * Method added to put the request to the outbound queue.
	 * @param req
	 */
	public void putOutboundContender(Request req){
		System.out.println("Inside the WhenServerBecomesClient.outOutboundContender()...");
		if(req == null)
			System.out.println("Request is null");
		try {
			// enqueue message
			outbound.put(req);
			System.out.println("Exiting the WhenServerBecomesClient.outOutboundContender()...");
		} catch (InterruptedException e) {
			logger.warn("Unable to deliver message, queuing");
		}
	}
	
	/**
	 * The method would be called to initiate a connection through bootstrap.
	 * Similar to ClientConnection, but this time it would be to solve when Server becomes client.
	 */
	private void init() {
		System.out.println("Inside the WhenServerBecomesClient.init()...");
		// Configure the client.
		bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool()));

		bootstrap.setOption("connectTimeoutMillis", 10000);
		bootstrap.setOption("tcpNoDelay", true);
		bootstrap.setOption("keepAlive", true);

		// Set up the pipeline factory.
		bootstrap.setPipelineFactory(new WhenServerBecomesClientDecoderPipeline());
		System.out.println("Exiting the WhenServerBecomesClient.init()...");
	}
	
	/**
	 * create connection to remote server
	 * 
	 * @return
	 */
	public Channel connect() { 
		System.out.println("Inside the WhenServerBecomesClient.connect()...");
		// Start the connection attempt.
		if (channelFuture == null) {
			logger.info("---> connecting to:::" + port + " from ServerAsClient");
			channelFuture = bootstrap
					.connect(new InetSocketAddress(host, port));
			// cleanup on lost connection
		}

		// wait for the connection to establish
		channelFuture.awaitUninterruptibly();
		if (channelFuture.isDone() && channelFuture.isSuccess()) {

			System.out.println("Exiting the WhenServerBecomesClient.connect()...");
			return channelFuture.getChannel();
		} else
			throw new RuntimeException(
					"Not able to establish connection to server");
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
		WhenServerBecomesClient conn;
		boolean forever = true;

		public OutboundWorker(WhenServerBecomesClient conn) {
			this.conn = conn;

			if (conn.outbound == null)
				throw new RuntimeException("connection worker detected null queue");
		}

		@Override
		public void run() {
			System.out.println("Inside the WhenServerBecomesClient.OutBoundWorker.run()...");
			Channel ch = conn.connect();
			if (ch == null || !ch.isOpen()) {
				WhenServerBecomesClient.logger.error("connection missing, no outbound communication");
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
					WhenServerBecomesClient.logger.error("Unexpected communcation failure", e);
					break;
				}
			}

			if (!forever) {
				WhenServerBecomesClient.logger.info("connection queue closing");
			}
			
			System.out.println("Exiting the WhenServerBecomesClient.OutBoundWorker.run()...");
		}
	}
}
