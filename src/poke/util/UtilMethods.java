package poke.util;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.client.ClientConnection;

import com.google.protobuf.ByteString;

public class UtilMethods {
	protected static Logger logger = LoggerFactory.getLogger(UtilMethods.class);

	public static void transferFile(ClientConnection cc, String filePath, String namespace, String originator){
		try{
			File file = new File(filePath);
			
			//First read the file from the disk before you can transfer it to node
			if(!file.exists())
			{
				logger.warn("No such file exists..."+filePath);
				return;
			}
			if(!file.getName().startsWith("."))
			{
				// To ignore system files
				logger.info("Copying file : "+file.getAbsolutePath()+" to host : "+
						cc.getHost()+", port : "+cc.getPort()+", originator : "+originator);
				
				DataInputStream inputStream = new DataInputStream(new FileInputStream(file));
				// Content of the file in byteArrayOutputStream
				ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
				IOUtils.copy(inputStream, byteArrayOutputStream);
				ByteString fileContent = ByteString.copyFrom(byteArrayOutputStream.toByteArray());
				
				// After reading the file transfer it to the node using protobuf
				cc.sendFile(file.getName(), fileContent, namespace, originator);
				
				IOUtils.closeQuietly(inputStream);
				IOUtils.closeQuietly(byteArrayOutputStream);
			}
		}
		catch(IOException e){
			System.err.println("An error occured file sending the file !!!");
			e.printStackTrace();
		}
	}
}
