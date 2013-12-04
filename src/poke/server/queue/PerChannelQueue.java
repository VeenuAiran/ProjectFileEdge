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
package poke.server.queue;

import java.lang.Thread.State;
import java.util.concurrent.LinkedBlockingDeque;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.resources.Resource;
import poke.server.resources.ResourceFactory;
import poke.server.resources.ResourceUtil;
import poke.server.routing.ReqRespCorrelationIdMappingUtil;

import com.google.protobuf.GeneratedMessage;

import eye.Comm.Header.ReplyStatus;
import eye.Comm.Request;
import eye.Comm.Response;

/**
 * A server queue exists for each connection (channel). A per-channel queue
 * isolates clients. However, with a per-client model. The server is required to
 * use a master scheduler/coordinator to span all queues to enact a QoS policy.
 * 
 * How well does the per-channel work when we think about a case where 1000+
 * connections?
 * 
 * @author gash
 * 
 */
public class PerChannelQueue implements ChannelQueue {
	protected static Logger logger = LoggerFactory.getLogger(PerChannelQueue.class);

	private Channel channel;
	private LinkedBlockingDeque<com.google.protobuf.GeneratedMessage> inbound;
	private LinkedBlockingDeque<com.google.protobuf.GeneratedMessage> outbound;
	private OutboundWorker oworker;
	private InboundWorker iworker;

	// not the best method to ensure uniqueness
	private ThreadGroup tgroup = new ThreadGroup("ServerQueue-" + System.nanoTime());

	protected PerChannelQueue(Channel channel) {
		this.channel = channel;
		init();
	}

	protected void init() {
		inbound = new LinkedBlockingDeque<com.google.protobuf.GeneratedMessage>();
		outbound = new LinkedBlockingDeque<com.google.protobuf.GeneratedMessage>();

		iworker = new InboundWorker(tgroup, 1, this);
		iworker.start();

		oworker = new OutboundWorker(tgroup, 1, this);
		oworker.start();
	}

	protected Channel getChannel() {
		return channel;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see poke.server.ChannelQueue#shutdown(boolean)
	 */
	@Override
	public void shutdown(boolean hard) {
		logger.info("server is shutting down");

		channel = null;

		if (hard) {
			// drain queues, don't allow graceful completion
			inbound.clear();
			outbound.clear();
		}

		if (iworker != null) {
			iworker.forever = false;
			if (iworker.getState() == State.BLOCKED || iworker.getState() == State.WAITING)
				iworker.interrupt();
			iworker = null;
		}

		if (oworker != null) {
			oworker.forever = false;
			if (oworker.getState() == State.BLOCKED || oworker.getState() == State.WAITING)
				oworker.interrupt();
			oworker = null;
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see poke.server.ChannelQueue#enqueueRequest(eye.Comm.Finger)
	 */
	@Override
	public void enqueueRequest(Request req) {
		System.out.println("Inside the PerChannelQueue.enqueueRequest()...");
		try {
			inbound.put(req);
			System.out.println("Exiting the PerChannelQueue.enqueueRequest()...Message enqueued for processing.");
		} catch (InterruptedException e) {
			logger.error("message not enqueued for processing", e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see poke.server.ChannelQueue#enqueueResponse(eye.Comm.Response)
	 */
	@Override
	public void enqueueResponse(Response reply) {
		System.out.println("Inside the PerChannelQueue.enqueueResponse()...");
		if (reply == null)
			return;

		try {
			System.out.println("Reply added to the outbound queue to send the reply...");
			System.out.println("Inside the PerChannelQueue.enqueueResponse()...");
			outbound.put(reply);
		} catch (InterruptedException e) {
			logger.error("message not enqueued for reply", e);
		}
	}

	protected class OutboundWorker extends Thread {
		int workerId;
		PerChannelQueue sq;
		boolean forever = true;

		public OutboundWorker(ThreadGroup tgrp, int workerId, PerChannelQueue sq) {
			super(tgrp, "outbound-" + workerId);
			this.workerId = workerId;
			this.sq = sq;

			if (outbound == null)
				throw new RuntimeException("connection worker detected null queue");
		}

		@Override
		public void run() {
			Channel conn = sq.channel;
			if (conn == null || !conn.isOpen()) {
				PerChannelQueue.logger.error("connection missing, no outbound communication");
				return;
			}

			while (true) {
				System.out.println("Inside the PerChannelQueue.OutboundWorker.run().while(true)...");
				if (!forever && sq.outbound.size() == 0)
					break;

				try {
					// block until a message is enqueued
					GeneratedMessage msg = sq.outbound.take();
					//Adding a null condition check on the msg that would returned as response when the first request to
					//local server is not satisfied.
					if(msg == null){
						System.out.println("The Request could not be satisfied. So possibly could get forwarded.");
					}else{
						System.out.println("We finally got the response for the request. May be from another server.");
					if (conn.isWritable()) {
						System.out.println("Inside of IF");
						boolean rtn = false;
						if (channel != null && channel.isOpen() && channel.isWritable()) {
							System.out.println("Inside of IF2");
							ChannelFuture cf = channel.write(msg);

							// blocks on write - use listener to be async
							cf.awaitUninterruptibly();
							rtn = cf.isSuccess();
							if (!rtn){
								System.out.println("Inside of IF3");
								sq.outbound.putFirst(msg);
							}
						}

					} else
						sq.outbound.putFirst(msg);
					}
				} catch (InterruptedException ie) {
					break;
				} catch (Exception e) {
					PerChannelQueue.logger.error("Unexpected communcation failure", e);
					break;
				}
				System.out.println("Exiting the PerChannelQueue.OutboundWorker.run().while(true)...");
			}

			if (!forever) {
				PerChannelQueue.logger.info("connection queue closing");
			}
		}
	}

	protected class InboundWorker extends Thread {
		int workerId;
		PerChannelQueue sq;
		boolean forever = true;

		public InboundWorker(ThreadGroup tgrp, int workerId, PerChannelQueue sq) {
			super(tgrp, "inbound-" + workerId);
			this.workerId = workerId;
			this.sq = sq;

			if (outbound == null)
				throw new RuntimeException("connection worker detected null queue");
		}

		@Override
		public void run() {
			System.out.println("Inside the PerChannelQueue.InBoundWorker.run()...");
			Channel conn = sq.channel;
			if (conn == null || !conn.isOpen()) {
				PerChannelQueue.logger.error("connection missing, no inbound communication");
				return;
			}

			while (true) {
				System.out.println("Inside the PerChannelQueue.InboundWorker.run().while(true)...");
				if (!forever && sq.inbound.size() == 0)
					break;

				try {
					// block until a message is enqueued
					GeneratedMessage msg = sq.inbound.take();

					// process request and enqueue response
					if (msg instanceof Request) {
						Request req = ((Request) msg);

						// do we need to route the request?
						// We do route the request if the local server or the first recipient server cannot process the request.
						// This functionality has been taken care in DocumentResource and ForwardResource class.
						// handle it locally
						Resource rsc = ResourceFactory.getInstance().resourceInstance(req.getHeader());

						Response reply = null;
						if (rsc == null) {
							logger.error("failed to obtain resource for " + req);
							reply = ResourceUtil.buildError(req.getHeader(), ReplyStatus.FAILURE,
									"Request not processed");
						} else
							reply = rsc.process(req);
						

						System.out.println("");
						System.out.println("Exiting the PerChannelQueue.InboundWorker.run().while(true)...");
						sq.enqueueResponse(reply);
					}

				} catch (InterruptedException ie) {
					break;
				} catch (Exception e) {
					PerChannelQueue.logger.error("Unexpected processing failure", e);
					break;
				}
			}

			if (!forever) {
				PerChannelQueue.logger.info("connection queue closing");
			}
		}
	}

	public class CloseListener implements ChannelFutureListener {
		private ChannelQueue sq;

		public CloseListener(ChannelQueue sq) {
			this.sq = sq;
		}

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			sq.shutdown(true);
		}
	}
}
