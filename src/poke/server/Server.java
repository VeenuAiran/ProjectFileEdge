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
package poke.server;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.bootstrap.Bootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.AdaptiveReceiveBufferSizePredictorFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.client.ClientConnection;
import poke.client.ClientListener;
import poke.client.ClientPrintListener;
import poke.server.conf.ExternalNodeInfo;
import poke.server.conf.ExternalNodeMapping;
import poke.server.conf.JsonUtil;
import poke.server.conf.NodeDesc;
import poke.server.conf.ServerConf;
import poke.server.management.HeartbeatData;
import poke.server.management.HeartbeatConnector;
import poke.server.management.HeartbeatManager;
import poke.server.management.ManagementDecoderPipeline;
import poke.server.management.ManagementQueue;
import poke.server.resources.ResourceFactory;
import poke.server.routing.ServerDecoderPipeline;
import poke.server.storage.jdbc.DatabaseStorage;
import poke.util.UtilMethods;

/**
 * Note high surges of messages can close down the channel if the handler cannot
 * process the messages fast enough. This design supports message surges that
 * exceed the processing capacity of the server through a second thread pool
 * (per connection or per server) that performs the work. Netty's boss and
 * worker threads only processes new connections and forwarding requests.
 * <p>
 * Reference Proactor pattern for additional information.
 * 
 * @author gash
 * 
 */
public class Server {
	protected static Logger logger = LoggerFactory.getLogger("server");
	//Added for integration.
	public static int lastestFileIdInDB = 0;

	protected static final ChannelGroup allChannels = new DefaultChannelGroup("server");
	protected static HashMap<Integer, Bootstrap> bootstrap = new HashMap<Integer, Bootstrap>();
	protected ChannelFactory cf, mgmtCF;
	protected ServerConf conf;
	protected HeartbeatManager hbMgr;
	public static String ownNodeId = "";
	
	//Added for Integration
	public static String COMMON_LOCATION = "/home/krish/Documents/";
	public static String SERVER_NAME;
	public static String DEFAULT_NAMESPACE = "default";
	
	//Added for Integration.
	protected DatabaseStorage databaseStorage = null;
	public static Map<String, ClientConnection> neighborToCcMap = new HashMap<String, ClientConnection>();
	public ExternalNodeMapping extNodeMap = new ExternalNodeMapping();

	/**
	 * static because we need to get a handle to the factory from the shutdown
	 * resource
	 */
	public static void shutdown() {
		try {
			ChannelGroupFuture grp = allChannels.close();
			grp.awaitUninterruptibly(5, TimeUnit.SECONDS);
			for (Bootstrap bs : bootstrap.values())
				bs.getFactory().releaseExternalResources();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		logger.info("Server shutdown");
		System.exit(0);
	}

	/**
	 * initialize the server with a configuration of it's resources
	 * 
	 * @param cfg
	 */
	public Server(File cfg) {
		// get the file name of conf as it determine which server we are
		String confFileName = cfg.getName();
		SERVER_NAME = confFileName.replace(".conf", "");
		logger.info("Current server name : "+SERVER_NAME);
		init(cfg);
	}

	private void init(File cfg) {
		// resource initialization - how message are processed
		BufferedInputStream br = null;
		try {
			byte[] raw = new byte[(int) cfg.length()];
			br = new BufferedInputStream(new FileInputStream(cfg));
			br.read(raw);
			conf = JsonUtil.decode(new String(raw), ServerConf.class);
			ResourceFactory.initialize(conf);
			
			// establish nearest nodes and start receiving heartbeats
			String str = conf.getServer().getProperty("node.id");
			ownNodeId = str;
			logger.info("Server id is : "+ownNodeId);
			
			//Added for Integration.
			// Instantiate database storage class to work with db
			databaseStorage = new DatabaseStorage();
			
		} catch (Exception e) {
		}

		// communication - external (TCP) using asynchronous communication
		cf = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());

		// communication - internal (UDP)
		// mgmtCF = new
		// NioDatagramChannelFactory(Executors.newCachedThreadPool(),
		// 1);

		// internal using TCP - a better option
		mgmtCF = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newFixedThreadPool(2));

	}

	public void release() {
		if (hbMgr != null)
			hbMgr.release();
	}

	/**
	 * initialize the outward facing (public) interface
	 * 
	 * @param port
	 *            The port to listen to
	 */
	private void createPublicBoot(int port) {
		System.out.println("Inside the Server.createPublicBoot()...");
		// construct boss and worker threads (num threads = number of cores)

		ServerBootstrap bs = new ServerBootstrap(cf);

		// Set up the pipeline factory.
		bs.setPipelineFactory(new ServerDecoderPipeline());

		// tweak for performance
		bs.setOption("child.tcpNoDelay", true);
		bs.setOption("child.keepAlive", true);
		bs.setOption("receiveBufferSizePredictorFactory", new AdaptiveReceiveBufferSizePredictorFactory(1024 * 2,
				1024 * 4, 1048576));

		bootstrap.put(port, bs);

		// Bind and start to accept incoming connections.
		Channel ch = bs.bind(new InetSocketAddress(port));
		allChannels.add(ch);

		// We can also accept connections from a other ports (e.g., isolate read
		// and writes)

		logger.info("Starting server, listening on port = " + port);
		System.out.println("Starting server, listening on port = " + port);
		System.out.println("Exiting the Server.createPublicBoot()...");
	}

	/**
	 * initialize the private network/interface
	 * 
	 * @param port
	 *            The port to listen to
	 */
	private void createManagementBoot(int port) {
		// construct boss and worker threads (num threads = number of cores)

		// UDP: not a good option as the message will be dropped
		// ConnectionlessBootstrap bs = new ConnectionlessBootstrap(mgmtCF);

		// TCP
		System.out.println("Entered the Server.createManagementBoot()...");
		ServerBootstrap bs = new ServerBootstrap(mgmtCF);

		// Set up the pipeline factory.
		bs.setPipelineFactory(new ManagementDecoderPipeline());

		// tweak for performance
		// bs.setOption("tcpNoDelay", true);
		bs.setOption("child.tcpNoDelay", true);
		bs.setOption("child.keepAlive", true);

		bootstrap.put(port, bs);

		// Bind and start to accept incoming connections.
		Channel ch = bs.bind(new InetSocketAddress(port));
		allChannels.add(ch);

		logger.info("Starting server, listening on port = " + port);
		System.out.println("Starting server, listening on port = " + port);
		System.out.println("Exiting the Server.createManagementBoot()...");
	}

	/**
	 * The entry point for the thread to start running.
	 */
	public void run() {
		String str = conf.getServer().getProperty("port");
		if (str == null) {
			// TODO if multiple servers can be ran per node, assigning a default
			// is not a good idea
			logger.warn("Using default port 5570, configuration contains no port number");
			str = "5570";
		}

		int port = Integer.parseInt(str);

		str = conf.getServer().getProperty("port.mgmt");
		int mport = Integer.parseInt(str);

		// storage initialization
		// TODO storage setup (e.g., connection to a database)

		// start communication
		createPublicBoot(port);
		createManagementBoot(mport);

		// start management
		ManagementQueue.startup();

		hbMgr = HeartbeatManager.getInstance(str);
		for (NodeDesc nn : conf.getNearest().getNearestNodes().values()) {
			HeartbeatData node = new HeartbeatData(nn.getNodeId(), nn.getHost(), nn.getPort(), nn.getMgmtPort());
			System.out.println("Node ID :: "+nn.getNodeId()+" Host :: "+nn.getHost()+" Port :: "+nn.getPort()+" MgmtPort :: "+nn.getMgmtPort());
			HeartbeatConnector.getInstance().addConnectToThisNode(node);
		}
		
		//Code added to fetch the external node information and map it. Connection not created until there is a need.
		for (NodeDesc nn : conf.getExternal().getExternalNodes().values()) {
			ExternalNodeInfo node = new ExternalNodeInfo(nn.getNodeId(), nn.getHost(), nn.getPort());
			System.out.println("Node ID :: "+nn.getNodeId()+" Host :: "+nn.getHost()+" Port :: "+nn.getPort());
			extNodeMap.addExternalNodeInfoToMap(node);
		}
		//Code addition completed.
		hbMgr.start();

		// manage hbMgr connections
		HeartbeatConnector conn = HeartbeatConnector.getInstance();
		conn.start();

		//Added for Integration.
		// manage neighbor doc transfer connections
		createNeighborConn();
		
		logger.info(SERVER_NAME+" is ready");
		logger.info("Server ready");
		System.out.println("Exiting the Server.run()...");
	}
	
	/**
	 * The method to add the neighbor node to map.
	 * @param neighborHostname
	 * @param neighborPort
	 * @param serverName
	 */
	private void addNeighborToMap(String neighborHostname, int neighborPort, String serverName){
		ClientConnection cc = ClientConnection.initConnection(neighborHostname, neighborPort);
		if(cc != null){
			logger.info("Connecting to neighborHostname : "+neighborHostname+", neighborPort : "+neighborPort);
			ClientListener listener = new ClientPrintListener("ClientPrintListener for "+neighborHostname+":"+neighborPort);
			cc.addListener(listener);
		}
		
		neighborToCcMap.put(neighborHostname+":"+neighborPort, cc);
	}
	
	/**
	 * The method to get the nearest nodes and add to map for caching the connection.
	 * Also, this methods has thread implementation to check if a particular node is up after failure and 
	 * then transfer the files to that particular node for replicating it. Eventually we end up having 
	 * 3 replicas for our node. The thread polls the databse to see which file did not get replicated 
	 * at a particular node.
	 */
	private void createNeighborConn() {
		// Build Neighbor to cc map
		for (NodeDesc nn : conf.getNearest().getNearestNodes().values()) {
			// Neighbor hostname and port
			String neighborHostname = nn.getHost();
			int neighborPort = nn.getPort();
			
			addNeighborToMap(neighborHostname, neighborPort, SERVER_NAME);
		}

		// Thread to Replicate all my files for which I am the owner to neighboring nodes
		Thread th = new Thread() {
			public void run() {
				while(true){
					try {
						Thread.sleep(5000); // sleep for 5 secs
						
						logger.info("Start the thread ....to look for files not completely replicated.");
						// First try and create connection with every neighboring node if it does
						//		not exist.
						for (NodeDesc nn : conf.getNearest().getNearestNodes().values()) {
							// Neighbor hostname and port 
							String neighborHostname = nn.getHost();
							int neighborPort = nn.getPort();

							String neighborKey = neighborHostname+":"+neighborPort;
							
							if(neighborToCcMap.get(neighborKey) == null)
							{
								addNeighborToMap(neighborHostname, neighborPort, SERVER_NAME);
							}
						}
						
						/* Get map of files along with the neighbors where they are already replicated 
						 * for those files which are not completely replicated.
						 * This map has fileId+"|"+fileName+"|"+filepath+"|"+namespace as key and 
						 * set of nodes where this file is already replicated as value
						 */
						Map<String, Set<String>> fileMap = databaseStorage.getListOfReplicatedNodes();
						for(Entry<String, Set<String>> fileMapEntry : fileMap.entrySet())
						{
							String fileKey = fileMapEntry.getKey();
							Set<String> neighbors = fileMapEntry.getValue();
							
							boolean fileReplicated = true;
							
							String fileId = null,filePath = null, namespace = null;
							
							// Go through all the neighbors to determine where to replicate
							for (NodeDesc nn : conf.getNearest().getNearestNodes().values()) {
								// Neighbor hostname and port 
								String neighborHostname = nn.getHost();
								int neighborPort = nn.getPort();
 
								String neighborKey = neighborHostname+":"+neighborPort;
								// Need to replicate
								if(!neighbors.contains(neighborKey))
								{
									if(neighborToCcMap.get(neighborKey) != null)
									{
										String[] splitKeys = fileKey.split("\\|");
										fileId = splitKeys[0];
										filePath = splitKeys[2];
										namespace = splitKeys[3];
										logger.info("Thread replicating file : "+filePath+", fileId : "+fileId+", namespace : "+namespace+", to : "+neighborKey);
										// Replicate the file to neighbor 
										// Do this after checking that connection still exists
										ClientConnection cc = neighborToCcMap.get(neighborKey);
										if(cc.isConnected())
										{
											UtilMethods.transferFile(cc, filePath, namespace, "SERVER");
											// insert into the replication table
											databaseStorage.addReplicatedDoc(Long.parseLong(fileId), neighborKey);
										}
										else
										{
											neighborToCcMap.put(neighborKey, null);
											fileReplicated = false;
										}
									}
									else
									{
										fileReplicated = false;
									}
								}
							}
							
							if(fileReplicated)
							{
								// update the table that replication is complete
								databaseStorage.updateReplicationStatus(Long.parseLong(fileId), true);
							}
						}
						
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					
//					boolean toBreak = true;
//					// Check if all the neighbor's got all the files
//					for(Entry<String, ClientConnection> entry : neighborToCcMap.entrySet())
//					{
//						if(entry.getValue() == null)
//						{
//							toBreak = false;
//						}
//					}
//					if(toBreak) {
//						logger.info("Thread work is done.");
//						break;
//					}
				}
			}
		};

		th.start();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length != 1) {
			System.err.println("Usage: java " + Server.class.getClass().getName() + " conf-file");
			System.exit(1);
		}

		File cfg = new File(args[0]);
		if (!cfg.exists()) {
			Server.logger.error("configuration file does not exist: " + cfg);
			System.exit(2);
		}

		Server svr = new Server(cfg);
		svr.run();
	}
}
