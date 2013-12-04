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
package poke.demo;

import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.client.ClientConnection;
import poke.client.ClientListener;
import poke.client.ClientPrintListener;
import poke.util.UtilMethods;

public class Jab {
	
	protected static Logger logger = LoggerFactory.getLogger(Jab.class);
	public String DEFAULT_SERVER_IP = "localhost";
	public int DEFAULT_PORT = 5570;
	public int clientToServerPortNo;
	public Scanner input;
	
	private String tag;
	private int count;
	private String fileName = "Text1.txt";
	boolean hasFile = false;

	public Jab(String tag) {
		this.tag = tag;
	}

	/**
	 * The run method for the thread.
	 */
	public void run() {
		logger.info("Inside the Jab.run() method...");
		
		//Integration of code.
		System.out.println("Enter Server IP: ");
		input = new Scanner(System.in);
		String serverIP = input.nextLine(); 
		
		System.out.println("Enter Port No.: ");
		clientToServerPortNo = Integer.parseInt(new Scanner(System.in).nextLine());
		
		//Use default values if Server-IP or Port is missing
		if(serverIP == null || serverIP.equals("")){
			serverIP = DEFAULT_SERVER_IP;
		}
		
		if(clientToServerPortNo == 0 || !(clientToServerPortNo == 5570 ||
				clientToServerPortNo == 5571 ||clientToServerPortNo == 5572 ||clientToServerPortNo == 5573)){
			clientToServerPortNo = 5570;
		}
		
		ClientConnection cc = ClientConnection.initConnection(serverIP, clientToServerPortNo);
		ClientListener listener = new ClientPrintListener("jab demo");
		cc.addListener(listener);
		
		//Commenting the code for integration.
		//cc.fetchFile("JAB", "logLoadTime.txt");
		//cc.queryForFile("JAB", "logLoadTime.txt");
		//cc.removeFile("JAB", "Node0.txt");
		
		//System.out.println("The server has the file "+hasFile);
		uploadFile(cc);
		
		logger.info("Exiting the Jab.run() method...");
	}
	
	/**
	 * The method to upload file to the system.
	 */
	public void uploadFile(ClientConnection cc){
		//logger.info("Uploading file to node-0 from client");
		
		//28 Oct - Code
		logger.info("Uploading file to port " + clientToServerPortNo + "from client");
		System.out.println("Enter FileName along with te FilePath: ");
		input = new Scanner(System.in);
		String filePath = input.nextLine();

		//String filePath = "/Users/amrita/Documents/writing-duplicate.ppt";	
		UtilMethods.transferFile(cc, filePath, "JAB", "CLIENT");		
	}

	/**
	 * The main function as entry point.
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			Jab jab = new Jab("jab");
			jab.run();

			// we are running asynchronously
			System.out.println("\nExiting in 5 seconds");
			Thread.sleep(5000);
			System.exit(0);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
