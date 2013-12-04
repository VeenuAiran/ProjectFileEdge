package poke.server.routing;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelState;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.Server;
import poke.server.queue.PerChannelQueue;

import com.google.protobuf.GeneratedMessage;

import eye.Comm.Request;

/**
 * This handler class will be in use when the server as a client while interacting with the adjacent
 * node(s). This hanlder class will also be useful when distinguishing the response and requests.
 * @author veenu
 *
 */
public class WhenServerBecomesClientHandler extends SimpleChannelUpstreamHandler{

	protected static Logger logger = LoggerFactory.getLogger("whenServerBecomesClientHandler");

	private volatile Channel channel;
	
	/**
	 * Public constructor.
	 */
	public WhenServerBecomesClientHandler(){
		
	}
	
	/**
	 * Method to send the message by writing on to channel. ChannelFuture is used to look for operation status.
	 * @param msg
	 * @return
	 */
	public boolean send(GeneratedMessage msg) {
		// TODO a queue is needed to prevent overloading of the socket
		// connection. For the demonstration, we don't need it
		ChannelFuture cf = channel.write(msg);
		if (cf.isDone() && !cf.isSuccess()) {
			logger.error("failed to poke!");
			return false;
		}

		return true;
	}
	
	/**
	 * The method added to had
	 * @param msg
	 */
	public void handleMessage(eye.Comm.Response msg) {
		System.out.println("Inside the WhenServerBecomesClientHandler.handleMessage()...");
		if (msg.getHeader().getRoutingId() == eye.Comm.Header.Routing.DOCFIND) {
			logger.info("Searching for the PCQ for corrID::"
					+ msg.getHeader().getCorrelationId() + " in the routing log"
					+ Server.ownNodeId + ":::"
					+ ReqRespCorrelationIdMappingUtil.getInstance().correlationMapping);
			
			if (ReqRespCorrelationIdMappingUtil.getInstance().correlationMapping.containsKey(Integer.parseInt(msg.getHeader()
					.getCorrelationId()))) {
				logger.info("nailed it!...");
				PerChannelQueue pcq = ReqRespCorrelationIdMappingUtil.getInstance().correlationMapping
						.get(Integer.parseInt(msg.getHeader().getCorrelationId()));
				logger.info("RESPONSE::" + msg.getHeader().getCorrelationId()
						+ " to be enqueued in outbound queue of " + pcq);
				pcq.enqueueResponse(msg);//Need to check how this works.
			}
		}
		System.out.println("Exiting the WhenServerBecomesClientHandler.handleMessage()...");
	}
	
	@Override
	public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		channel = e.getChannel();
		super.channelOpen(ctx, e);
	}
	
	@Override
	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		if (channel.isConnected())
			channel.write(ChannelBuffers.EMPTY_BUFFER).addListener(
					ChannelFutureListener.CLOSE);
	}

	@Override
	public void channelInterestChanged(ChannelHandlerContext ctx,
			ChannelStateEvent e) throws Exception {
		if (e.getState() == ChannelState.INTEREST_OPS
				&& ((Integer) e.getValue() == Channel.OP_WRITE)
				|| (Integer) e.getValue() == Channel.OP_READ_WRITE)
			logger.warn("channel is not writable! <--------------------------------------------");
	}
	
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
		logger.info("BINGO! message received in the WhenServerBecomesClient handler");
		if (e.getMessage() instanceof Request) {
			logger.info("Problem!");
		}
		handleMessage((eye.Comm.Response) e.getMessage());
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
		System.out.println("ERROR: " + e.getCause());

		// TODO do we really want to do this? try to re-connect?
		e.getChannel().close();
	}
}
