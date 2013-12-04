package poke.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.client.util.ClientUtil;
import eye.Comm.Header;

/**
 * example listener that an application would use to receive events.
 * 
 * @author gash
 * 
 */
public class ClientPrintListener implements ClientListener {
	protected static Logger logger = LoggerFactory.getLogger("client");

	private String id;

	public ClientPrintListener(String id) {
		this.id = id;
	}

	@Override
	public String getListenerID() {
		return id;
	}

	/**
	 * This method has been updated to get the response back and print the contents.
	 */
	@Override
	public void onMessage(eye.Comm.Response msg) {
		logger.info("Inside the ClientPrintListener.onMessage()....");
		if (logger.isDebugEnabled())
			ClientUtil.printHeader(msg.getHeader());
		
		if (msg.getHeader().getRoutingId() == Header.Routing.DOCFIND){
			if (msg.getHeader().getReplyCode().getNumber() != eye.Comm.Header.ReplyStatus.SUCCESS_VALUE){
				logger.info(" - Re Msg : " + msg.getHeader().getReplyMsg());
				ClientUtil.printHeader(msg.getHeader());
			}else{
				logger.info("Yes the request is for DOCFIND");
				for (int i = 0, I = msg.getBody().getDocsCount(); i < I; i++){
					logger.info("Document Name recieved at the client side :: "+msg.getBody().getDocs(i).getDocName());
					logger.info("Did we receive the bytes for document :: "+msg.getBody().getDocs(i).getChunkContent().size());
					ClientUtil.printDocument(msg.getBody().getDocs(i));
					
				}
			}			
		}else if (msg.getHeader().getRoutingId() == Header.Routing.DOCQUERY){
			if (msg.getHeader().getReplyCode().getNumber() != eye.Comm.Header.ReplyStatus.SUCCESS_VALUE){
				logger.info(" - Re Msg : " + msg.getHeader().getReplyMsg());
				ClientUtil.printHeader(msg.getHeader());
			}else{
				logger.info("Yes the request is for DOCQUERY");
				for (int i = 0, I = msg.getBody().getDocsCount(); i < I; i++){
					logger.info("Document Name recieved at the client side :: "+msg.getBody().getDocs(i).getDocName());
					logger.info("Did we receive the bytes for document :: "+msg.getBody().getDocs(i).getChunkContent().size());
					ClientUtil.printDocument(msg.getBody().getDocs(i));
				}
			}
		}else if(msg.getHeader().getRoutingId() == Header.Routing.DOCREMOVE){
			if (msg.getHeader().getReplyCode().getNumber() != eye.Comm.Header.ReplyStatus.SUCCESS_VALUE){
				logger.info(" - Re Msg : " + msg.getHeader().getReplyMsg());
				ClientUtil.printHeader(msg.getHeader());
			}
		}
		logger.info("Exiting the ClientPrintListener.onMessage()....");
	}
}
