package poke.client.util;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * This class is added to read a file and convert to byte.
 * Would be used whenever any file has to be read.
 * @author veenu
 *
 */
public class ReadFileToByte {

	/**
	 * This method will take File as input and convert into byte[].
	 * @param pFile
	 * @return byte[]
	 */
	public byte[] convertFileToByte(File pFile){
		//declare byte array of the size of the file to be read.
		System.out.println("Inside the ReadFileToByte.convertFileToByte()...");
		System.out.println("File Name :: "+pFile.getName());
		byte[] fileToByte = new byte[(int) pFile.length()];
		FileInputStream fis = null;
		BufferedInputStream bis = null;
		//Read the file into a byte array.
		try{
			fis = new FileInputStream(pFile);
			bis = new BufferedInputStream(fis);
			bis.read(fileToByte);
			System.out.println("File to byte completed...");
		}catch(IOException e){
			e.printStackTrace();
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			try{
				fis.close();
				bis.close();
			}catch(IOException e){
				e.printStackTrace();
			}
		}
		
		return fileToByte;
	}
}
