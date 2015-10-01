/**                                                                                                                                                                                
 * Copyright (c) 2014 USC Database Laboratory All rights reserved. 
 *
 * Authors:  Sumita Barahmand and Shahram Ghandeharizadeh                                                                                                                            
 *             http://bgbenchmark.org/BG/coordinator.html                                                                                                                                                                    
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */


import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Scanner;


class KillThread extends Thread {
	Scanner _coordScanner = null;
	PrintWriter _BGWriter = null;

	public KillThread(Scanner coordScanner, PrintWriter BGWriter) {
		_BGWriter = BGWriter;
		_coordScanner = coordScanner;
	}

	public void run() {
		while (_coordScanner.hasNext()) {
			String msg = _coordScanner.next();
			if (msg.equals("KILL")) {
				System.out
				.println("Coordinator wants the BG client to stop as it is not satisfying SLA requirements.");
				_BGWriter.print("KILL ");
				_BGWriter.flush();
				break;
			}
		}
	}
}

/**
 * Provides the communication between the BGClient and the BGCoordinator
 * @author Sumita Barahmand
 *
 */
public class BGListener {
	static final String REPEATED = "repeated";
	static final String ONETIME = "onetime";
	static final String BGMainClass = "edu.usc.bg.BGMainClass";
	static final String ENDOFMSG_IDENTIFIER = "#";
	static final String SHUTDOWNMSG = "shutdown";
	static final String BGCLIENTSTARTEDMSG = "Started";
	static final String LOADINGCOMPLETEDMSG = "LoadingCompleted "; 
	static final String INITIATEDMSG ="Initiated";

	static final String CONNECTEDMSG = "Connected";
	static final String STARTSIMULATIONMSG = "StartSimulation";
	static final String ENDMSG = "THEEND.";
	static final String EXEDONEMSG = "EXECUTIONDONE";



	static  String PORT = "port";
	static boolean BGClientStarted = false;
	static int BGClientPort = -1;
	InputStream stdout = null;
	BufferedReader reader = null;


	public static void main(String[] args) {

		ServerSocket BGCoordListenerSocket = null;
		Socket coordConnection = null;
		InputStream inputstreamFromCoord = null;
		OutputStream outputstreamToCoord = null;
		PrintWriter printerToCoord = null;
		Scanner scannerFromCoord = null;
		Process _BGProcess = null;
		Socket requestSocketToBGClient = null;
		OutputStream outputstreamToBG = null;
		InputStream inputstreamFromBG = null;
		InputStream BGClientStdout = null;
		BufferedReader readerFromBGClientStdout = null;
		PrintWriter printerToBGSocket = null;
		Scanner scannerFromBGSocket = null;

		HashMap<String, String> params = new HashMap<String, String>();
		boolean listenerRunning = true;

		if (args.length < 1) {
			System.out.println("The listenet's config file is missing.");
			System.exit(0);
		}

		String configFile = args[0];
		readParams(configFile, params);
		int port = Integer.parseInt(params.get(PORT));
		String toAdd = "";
		// Read listeners parameters
		Iterator<String> it = params.keySet().iterator();
		while (it.hasNext()) {
			String tmpkey = (String) it.next();
			String tmpval = params.get(tmpkey);
			if (tmpkey.equals(PORT))
				continue;
			else {
				toAdd += " -p " + tmpkey + "=" + tmpval;
			}
		}

		try {
			// create a socket on the port specified and wait for connections
			// listener waits for connection from the coordinator
			BGCoordListenerSocket = new ServerSocket(port, 10);
			System.out
			.println("BGListener: started and waiting for connection on "
					+ port);
			while (listenerRunning) {
				coordConnection = BGCoordListenerSocket.accept();
				System.out.println("BGListener: Connection received from "
						+ coordConnection.getInetAddress().getHostName());
				inputstreamFromCoord = coordConnection.getInputStream();
				outputstreamToCoord = coordConnection.getOutputStream();
				printerToCoord = new PrintWriter(new BufferedOutputStream(
						outputstreamToCoord));
				scannerFromCoord = new Scanner(inputstreamFromCoord);

				// read msg sent from the coord and pass it to the BGClient
				String BGParams = "", line = "";
				while (scannerFromCoord.hasNext()) {
					line = scannerFromCoord.next();
					if (!line.equals(ENDOFMSG_IDENTIFIER)) {
						line += " ";
						BGParams += line;
					} else {
						break;
					}
				}

				// check if BGClient has started, if not assign a port to it
				// find an open port on the local machine and send that as the
				// port the BGClient needs to listen on
				String BGline = "";
				if(BGParams.contains(ONETIME)){
					//shutdown the BGClient if it was running
					if(BGParams.contains(SHUTDOWNMSG)){
						System.out.println("BGListener received a shutdown msg, will wait for new round...");
						continue;
					}
					if(BGClientStarted){
						runWithRepeatedBG(SHUTDOWNMSG, coordConnection, printerToCoord, scannerFromCoord, toAdd, readerFromBGClientStdout);
						_BGProcess = null;
						readerFromBGClientStdout = null;
						BGClientStarted = false;
						BGClientPort = -1;
					}
					runWithOneTimeBG(BGParams, coordConnection, printerToCoord, scannerFromCoord, toAdd);
				}else{
					if(!BGClientStarted){
						ServerSocket server = new ServerSocket(0);
						BGClientPort = server.getLocalPort();
						server.setReuseAddress(true);
						server.close();
						if(BGParams.contains(SHUTDOWNMSG)){
							System.out.println("BGListener received a shutdown msg, will wait for new round...");
							continue;
						}
						String BGProcessCmd = BGParams.substring(0, BGParams.indexOf(BGMainClass)+BGMainClass.length())+" repeated "
								+ BGClientPort;

						_BGProcess = Runtime.getRuntime().exec(BGProcessCmd);
						BGClientStarted = true;
						System.out
						.println("BGLlistener: Starting the BGClient using:"
								+ BGProcessCmd);
						// start the communication to be able to send messages to
						// BGClient
						BGClientStdout = _BGProcess.getInputStream();
						readerFromBGClientStdout = new BufferedReader(
								new InputStreamReader(BGClientStdout));
						// wait for BG to start
						while ((BGline = readerFromBGClientStdout.readLine()) != null) {
							System.out.println("Stdout: " + BGline);
							if (BGline.equals(BGCLIENTSTARTEDMSG)) {
								System.out
								.println("Got \"started\" msg from the BG client so it has started and ready.");
								break;
							}
						}
					}
					runWithRepeatedBG(BGParams, coordConnection, printerToCoord, scannerFromCoord, toAdd, readerFromBGClientStdout);
					if(BGParams.contains(SHUTDOWNMSG)){
						_BGProcess = null;
						readerFromBGClientStdout = null;					
						BGClientStarted = false;
						BGClientPort = -1;
					}
				}
				//close connection to coord
				if (inputstreamFromCoord != null)
					inputstreamFromCoord.close();
				if (outputstreamToCoord != null)
					outputstreamToCoord.close();
				if (printerToCoord != null)
					printerToCoord.close();
				if (scannerFromCoord != null)
					scannerFromCoord.close();

			} //end of while for listener
		}catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			try {
				if (printerToBGSocket != null)
					printerToBGSocket.close();
				if (outputstreamToBG != null)
					outputstreamToBG.close();
				if (scannerFromBGSocket != null)
					scannerFromBGSocket.close();
				if (inputstreamFromBG != null)
					inputstreamFromBG.close();
				if (requestSocketToBGClient != null)
					requestSocketToBGClient.close();
				if (inputstreamFromCoord != null)
					inputstreamFromCoord.close();
				if (outputstreamToCoord != null)
					outputstreamToCoord.close();
				if (printerToCoord != null)
					printerToCoord.close();
				if (scannerFromCoord != null)
					scannerFromCoord.close();
				if (_BGProcess != null)
					_BGProcess.destroy();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void runWithOneTimeBG(String _BGParams, Socket _coordSocket, PrintWriter _prinerToCoord,Scanner _scannerFromCoord, String _toAdd) throws IOException, InterruptedException{

		if(_BGParams.contains("-load")){
			Process _BGLoadProcess = Runtime.getRuntime ().exec(_BGParams);
			InputStream stdout = _BGLoadProcess.getInputStream ();
			BufferedReader reader = new BufferedReader (new InputStreamReader(stdout));				
			String BGline ="";
			while ((BGline = reader.readLine ()) != null ) {
				System.out.println ("Stdout: "+ BGline);
			}
			//wait for the load to complete
			_BGLoadProcess.waitFor();
			_prinerToCoord.print(LOADINGCOMPLETEDMSG);
			_prinerToCoord.flush();
			System.out.println("BGListener: sent loading completed message to " + _coordSocket.getInetAddress().getHostName());
			if(stdout != null) stdout.close();
			if(reader != null) reader.close();
			if(_BGLoadProcess != null) _BGLoadProcess.destroy();
			System.out.println("BGlistener: ended loading data store will go wait for next request.");

		}else{//running becnhamrk
			//find an open port on the local machine and send that as the port the BGClient needs to listen on
			ServerSocket server =  new ServerSocket(0);
			int _BGport = server.getLocalPort();
			server.close();
			_BGParams +=" -p port="+_BGport+" "+_toAdd;
			System.out.println("BGLlistener: Starting the BGClient in onetime mode using:"+_BGParams);
			Socket requestSocket = null;
			OutputStream BGOut = null;
			InputStream BGIn = null;					
			Process _BGProcess = Runtime.getRuntime ().exec(_BGParams);

			InputStream stdout = _BGProcess.getInputStream ();
			BufferedReader reader = new BufferedReader (new InputStreamReader(stdout));				
			String BGline ="";
			while ((BGline = reader.readLine ()) != null ) {
				System.out.println ("Stdout: "+ BGline);
				if(BGline.equals(BGCLIENTSTARTEDMSG)){
					System.out.println("Got \"started\" msg from the BG client so it has started and ready.");
					break;
				}
			}

			//try connecting to the BG client's socket
			requestSocket = new Socket("localhost",_BGport);
			System.out.println("BGListener:Listener connection socket created to BG client on port "+_BGport);			
			BGOut = requestSocket.getOutputStream();
			BGIn =  requestSocket.getInputStream();
			PrintWriter BGOutPrint = new PrintWriter(BGOut);
			Scanner BGInScan = new Scanner(BGIn);
			//wait to see if connected
			//wait to get connected msg which shows the BG client process has started
			String msg = "";
			while(BGInScan.hasNext()){
				msg = BGInScan.next();
				if(msg.equalsIgnoreCase(INITIATEDMSG)){
					System.out.println("BGListener:The connection initiation between listener and BGClient is established");
					break;
				}
			}

			while ((BGline = reader.readLine ()) != null ) {
				System.out.println("stdout:"+BGline);
				if(BGline.equals(CONNECTEDMSG))
					break;
			}
			//now we can tell the coordinator that everything is ready
			_prinerToCoord.print(CONNECTEDMSG+" ");
			_prinerToCoord.flush();
			System.out.println("BGListener: sent connected message to " + _coordSocket.getInetAddress().getHostName());

			//wait till you get the start message from the coordinator
			String line = "";
			while(_scannerFromCoord.hasNext()){
				line = _scannerFromCoord.next();
				if(line.equals(STARTSIMULATIONMSG)){
					break;
				}
			}
			System.out.println("BGListener:Listener receieved the start simulation msg from the coordinator and needs to fwd it to the BGClient.");

			//listener fwds the startsimulation message to the BG client
			BGOutPrint.print(STARTSIMULATIONMSG+" ");
			BGOutPrint.flush();
			System.out.println("BGListener: sent StartSimulation message to BG Client on "+_BGport);
			System.out.println("BGListener:Waiting for stats from the BG client...");

			//create a thread to listen to kill msgs coming from the coord right once the simulatuion has started
			KillThread killThread = new KillThread( _scannerFromCoord ,BGOutPrint );
			killThread.start();


			BGline ="";
			boolean gotThru = false;
			boolean gotLatency = false;
			boolean gotActThru = false;
			String monitorToFlush = "";
			boolean killed = false;
			while ((BGline = reader.readLine ()) != null ) {
				if(gotLatency && gotThru && gotActThru){
					_prinerToCoord.print(monitorToFlush);
					_prinerToCoord.flush();
					gotLatency = false;
					gotThru = false;
					gotActThru=false;
					monitorToFlush = "";
				}
				System.out.println ("Stdout: "+ BGline);
				if(BGline.contains("MONITOR-THROUGHPUT(SESSIONS/SEC):")){
					gotThru = true;
					monitorToFlush+=BGline+" ";
				}else if(BGline.contains("MONITOR-THROUGHPUT(ACTIONS/SEC):")){
					gotActThru = true;
					monitorToFlush+=BGline+" ";
				}else if(BGline.contains("MONITOR-SATISFYINGOPS(%):")){
					monitorToFlush+=BGline+" ";
					gotLatency = true;
				}
				if(BGline.equals("DONE")){
					break;
				}else if(BGline.equals("KILLDONE")){
					killed = true;
					break;
				}
			}
			if(!killed){
				//listener waits to hear back from the BG client with its stats
				String stats = "";
				while(BGInScan.hasNext()){
					String tmpLine = BGInScan.next(); 
					System.out.println(tmpLine);
					stats += (tmpLine+" ");
					//System.out.println("####"+stats);
					if(tmpLine.equals(ENDMSG))
						break;
				}


				while ((BGline = reader.readLine ()) != null ) {
					System.out.println ("Stdout: "+ BGline);
				}

				_BGProcess.waitFor();
				//wait for the BG client process to end
				//send stats back to the coordinaror
				_prinerToCoord.print(stats);
				_prinerToCoord.flush();
				System.out.println("BGListener: sent stats to coordinator will wait for new round.");
			}else{
				if(_BGProcess != null)
					_BGProcess.destroy();
				killed = false;
				BGClientStarted = false;
				BGClientPort = -1;
				System.out.println("BGListener: killed the client will wait for new round.");
			}

			//kill the killThread if not killed
			if(killThread != null && !killThread.isInterrupted())
				killThread.interrupt();

			//clear the connection with the BG client
			if(BGOutPrint != null) BGOutPrint.close();
			if(BGOut != null) BGOut.close();
			if(BGInScan != null) BGInScan.close();
			if(BGIn != null) BGIn.close();
			if(requestSocket != null) requestSocket.close();
			if(_BGProcess != null) _BGProcess.destroy();
		}//end of else run benchmark
	}


	public static void runWithRepeatedBG(String _BGParams, Socket _coordSocket, PrintWriter _printerToCoord,Scanner _scannerFromCoord, String _toAdd, BufferedReader _readerFromBGClientStdout) throws UnknownHostException, IOException, InterruptedException{
		// send the message to BGClient
		Socket requestSocketToBGClient = new Socket("localhost", BGClientPort);
		System.out
		.println("BGListener:Listener connection socket created to BGClient on port "
				+ BGClientPort);
		OutputStream outputstreamToBG = requestSocketToBGClient.getOutputStream();
		InputStream inputstreamFromBG = requestSocketToBGClient.getInputStream();
		PrintWriter printerToBGSocket = new PrintWriter(outputstreamToBG);
		Scanner scannerFromBGSocket = new Scanner(inputstreamFromBG);
		_BGParams += " -p port=" + BGClientPort + " " + _toAdd;
		_BGParams += " # "; // as it's gonna be sent to BGClient
		System.out.println("****Msg fwded to BGClient:" + _BGParams);
		printerToBGSocket.print(_BGParams);
		printerToBGSocket.flush();

		// wait for the BGClient to complete execution
		String BGline = "";
		if (_BGParams.contains("-load")) {
			while ((BGline = _readerFromBGClientStdout.readLine()) != null) {
				System.out.println("Stdout: " + BGline);
				if (BGline.equals(EXEDONEMSG))
					break;
			}
			_printerToCoord.print(LOADINGCOMPLETEDMSG);
			_printerToCoord.flush();
			System.out
			.println("BGListener: sent loading completed message to "
					+ _coordSocket.getInetAddress()
					.getHostName()
					+ "...");
		}else if(_BGParams.contains(SHUTDOWNMSG)){ 
			while ((BGline = _readerFromBGClientStdout.readLine()) != null) {
				System.out.println("Stdout: " + BGline);
				if (BGline.contains("Bye!"))
					break;
			}
			System.out
			.println("BGListener: BGClient shutdown gracefully...");
		}else {
			// wait to see if connected
			// wait to get initiated msg which shows the BGClient
			// process is ready to listen
			while ((BGline = _readerFromBGClientStdout.readLine()) != null) {
				System.out.println("stdout:" + BGline);
				if (BGline.equals(INITIATEDMSG)) {
					System.out
					.println("BGListener:The connection initiation between listener and BGClient is established");
					// break;
				}
				if (BGline.equals(CONNECTEDMSG)) {
					_printerToCoord.print(CONNECTEDMSG+" ");
					_printerToCoord.flush();
					System.out
					.println("BGListener: sent connected message to "
							+ _coordSocket.getInetAddress()
							.getHostName());
					break;
				}
			}

			// wait till you get the start message from the coordinator
			String line ="";
			while (_scannerFromCoord.hasNext()) {
				line = _scannerFromCoord.next();
				if (line.equals(STARTSIMULATIONMSG)) {
					break;
				}
			}
			System.out
			.println("BGListener:Listener receieved the start simulation msg from the coordinator and needs to fwd it to the BGClient.");

			// listener fwds the startsimulation message to the BG
			// client
			printerToBGSocket.print(STARTSIMULATIONMSG+" ");
			printerToBGSocket.flush();
			System.out
			.println("BGListener: sent StartSimulation message to BG Client on "
					+ BGClientPort);
			System.out
			.println("BGListener:Waiting for stats from the BG client...");

			// create a thread to listen to kill msgs coming from the
			// coord right once the simulatuion has started
			KillThread killThread = new KillThread(_scannerFromCoord,
					printerToBGSocket);
			killThread.start();

			// reading stats from BGClient
			BGline = "";
			boolean gotThru = false;
			boolean gotLatency = false;
			boolean gotActThru = false;
			String monitorToFlush = "";
			boolean killed = false;
			while ((BGline = _readerFromBGClientStdout.readLine()) != null) {
				if (gotLatency && gotThru && gotActThru) {
					_printerToCoord.print(monitorToFlush);
					_printerToCoord.flush();
					gotLatency = false;
					gotThru = false;
					gotActThru = false;
					monitorToFlush = "";
				}
				System.out.println("Stdout: " + BGline);
				if (BGline
						.contains("MONITOR-THROUGHPUT(SESSIONS/SEC):")) {
					gotThru = true;
					monitorToFlush += BGline + " ";
				} else if (BGline
						.contains("MONITOR-THROUGHPUT(ACTIONS/SEC):")) {
					gotActThru = true;
					monitorToFlush += BGline + " ";
				} else if (BGline.contains("MONITOR-SATISFYINGOPS(%):")) {
					monitorToFlush += BGline + " ";
					gotLatency = true;
				}
				if (BGline.equals("DONE")) {
					break;
				} else if (BGline.equals("KILLDONE")) {
					killed = true;
					break;
				}
			}
			if (!killed) {
				// listener waits to hear back from the BG client with
				// its stats
				String stats = "";
				while (scannerFromBGSocket.hasNext()) {
					String tmpLine = scannerFromBGSocket.next();
					System.out.println(tmpLine);
					stats += (tmpLine + " ");
					// System.out.println("####"+stats);
					if (tmpLine.equals(ENDMSG))
						break;
				}

				while ((BGline = _readerFromBGClientStdout.readLine()) != null) {
					System.out.println("Stdout: " + BGline);
					if (BGline.equals(EXEDONEMSG))
						break;
				}

				// wait for the BG client process to end
				// send stats back to the coordinaror
				_printerToCoord.print(stats);
				_printerToCoord.flush();
				System.out
				.println("BGListener: sent stats to coordinator ....");
			}else{
				killed = false;
				BGClientStarted = false;
				BGClientPort = -1;
				System.out.println("BGListener: killed the client will wait for new round.");	
			}

			// kill the killThread if not killed
			if (killThread != null && !killThread.isInterrupted())
				killThread.interrupt();

		}
		if (printerToBGSocket != null)
			printerToBGSocket.close();
		if (outputstreamToBG != null)
			outputstreamToBG.close();
		if (scannerFromBGSocket != null)
			scannerFromBGSocket.close();
		if (inputstreamFromBG != null)
			inputstreamFromBG.close();
		if (requestSocketToBGClient != null)
			requestSocketToBGClient.close();
		Thread.sleep(5000);
		System.out
		.println("BGListener: waiting for new round....");

	}


	public static void readParams(String fileName,
			HashMap<String, String> params) {
		DataInputStream in = null;
		BufferedReader br = null;
		try {
			FileInputStream fstream = new FileInputStream(fileName);
			in = new DataInputStream(fstream);
			br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			while ((strLine = br.readLine()) != null) {
				if (strLine.trim() == "")
					continue;
				else {
					params.put(strLine.substring(0, strLine.indexOf("=")),
							strLine.substring(strLine.indexOf("=") + 1));
				}
			}
		} catch (Exception e) {// Catch exception if any
			System.out.println("Error: " + e.getMessage());
		} finally {
			try {
				if (in != null)
					in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
