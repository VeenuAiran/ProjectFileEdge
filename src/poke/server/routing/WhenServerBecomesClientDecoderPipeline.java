package poke.server.routing;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;

/**
 * This class will act as a decoder pipepline when server becomes client while interacting with 
 * adjacent node(s).  
 * @author veenu
 *
 */
public class WhenServerBecomesClientDecoderPipeline implements ChannelPipelineFactory {

	/*
	 * Constructor.
	 */
	public WhenServerBecomesClientDecoderPipeline(){
		
	}
	
	/**
	 * This method would return the ChannelPipeline that was created when this node made connection
	 * with the adjacent node(s).
	 * @return
	 * @throws Exception
	 */
	public ChannelPipeline getPipeline() throws Exception {
		System.out.println("Inside the WhenServerBecomesClientDecoderPipeline.getPipeline()...");
		ChannelPipeline pipeline = Channels.pipeline();

		// length (4 bytes)
		// max message size is 64 Mb = 67108864 bytes
		// this defines a framer with a max of 64 Mb message, 4 bytes are the
		// length, and strip 4 bytes
		pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(
				67108864, 0, 4, 0, 4));
		// pipeline.addLast("frameDecoder", new DebugFrameDecoder(67108864, 0,
		// 4, 0, 4));
		pipeline.addLast("protobufDecoder", new ProtobufDecoder(
				eye.Comm.Response.getDefaultInstance()));
		pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
		pipeline.addLast("protobufEncoder", new ProtobufEncoder());

		// varint framing - java-to-java
		// pipeline.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
		// pipeline.addLast("protobufDecoder", new
		// ProtobufDecoder(eye.Comm.Finger.getDefaultInstance()));
		// pipeline.addLast("frameEncoder", new
		// ProtobufVarint32LengthFieldPrepender());
		// pipeline.addLast("protobufEncoder", new ProtobufEncoder());

		// our message processor (new instance for each connection)
		pipeline.addLast("handler", new WhenServerBecomesClientHandler());

		System.out.println("Exiting the WhenServerBecomesClientDecoderPipeline.getPipeline()...");
		return pipeline;
	}
}
