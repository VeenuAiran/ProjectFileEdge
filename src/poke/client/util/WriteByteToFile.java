package poke.client.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import poke.constants.PokeNettyConstants;

/**
 * This class is to write the bytes to the file at client location.
 * @author veenu
 *
 */
public class WriteByteToFile {

	public String convertByteArrayToFile(byte[] byteArray, String pFileName){
		FileOutputStream fos = null;
		File downLoadFile  = new File(PokeNettyConstants.CLIENT_PATH_TOSTORE+pFileName);
		
		try{
			downLoadFile.createNewFile();
			fos = new FileOutputStream(downLoadFile);
			fos.write(byteArray);
		}catch(IOException e){
			e.printStackTrace();
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			try{
				fos.close();
			}catch(IOException e){
				e.printStackTrace();
			}
			
		}
		
		return "File Downloaded ON :: "+PokeNettyConstants.CLIENT_PATH_TOSTORE;
	}
}
