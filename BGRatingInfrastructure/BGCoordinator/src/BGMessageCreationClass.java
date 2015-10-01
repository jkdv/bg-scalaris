/**                                                                                                                                                                                
 * Copyright (c) 2014 USC Database Laboratory All rights reserved. 
 *
 * Authors:  Sumita Barahmand and Shahram Ghandeharizadeh                                                                                                                            
 *      http://bgbenchmark.org/BG/coordinator.html                                                                                                                                                                           
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;


/**
 * Creates the messages required to be sent to the BGClients
 * 
 * @author Sumita Barahmand
 *
 */
public class BGMessageCreationClass {

	/**
	 * Constructs the command issued only by the coordinator to create the schema for the data store
	 * @return
	 */
	public static String createSchemaCommand() {
		String token = "-schema";
		String schemaCmd = "";
		schemaCmd = Coordinator.commonProps.get(CoordinatorConstants.EXEPARAM_PROP) + " onetime " + token
				+ " -p "+CoordinatorConstants.RATINGMODE_PROP+"=false ";

		Set<String> commonsKeys = Coordinator.commonProps.keySet();
		Iterator<String> it = commonsKeys.iterator();
		while (it.hasNext()) {
			String tmpKey = it.next();
			if (tmpKey.equals(CoordinatorConstants.EXEPARAM_PROP) || tmpKey.equals(CoordinatorConstants.POPULATEFILE_PROP)
					|| tmpKey.equals(CoordinatorConstants.EXETIME_PROP)
					|| tmpKey.equals(CoordinatorConstants.THREADCOUNT_PROP)) {
				continue;
			} else if (tmpKey.equals(CoordinatorConstants.DB_PROP)) {
				schemaCmd += " -db " + Coordinator.commonProps.get(CoordinatorConstants.DB_PROP);
			} else {
				String val = Coordinator.commonProps.get(tmpKey);
				schemaCmd += " -p " + tmpKey + "=" + val;
			}
		}
		// add the schema specific parameters
		Set<String> schemaKeys = Coordinator.schemaProps.keySet();
		it = schemaKeys.iterator();
		while (it.hasNext()) {
			String tmpKey = (String) it.next();
			String val = Coordinator.schemaProps.get(tmpKey);
			if (tmpKey.equals(CoordinatorConstants.DB_PROP))
				schemaCmd += " -db " + val;
			else
				schemaCmd += " -p " + tmpKey + "=" + val;
		}
		return schemaCmd;
	}

	/**
	 * Constructs the load message that is sent to each BGClient
	 * @param threadid, BGClient's machine id
	 * @param usercount, total number of users in the social graph
	 * @param useroffsets
	 * @return
	 */
	public static String createBGThreadLoadMsg(int threadid, String usercount,
			String useroffset) {

		String loadMsg = Coordinator.commonProps.get(CoordinatorConstants.EXEPARAM_PROP) + " " + " onetime "
				+ " -loadindex " + " -P "
				+ Coordinator.commonProps.get(CoordinatorConstants.POPULATEFILE_PROP)
				+ " -p "+CoordinatorConstants.MACHINEID_PROP+"=" + threadid + " -p "+CoordinatorConstants.RATINGMODE_PROP+"=false";

		// add the common properties needed by the BGClients
		Set<String> params = Coordinator.commonProps.keySet();
		Iterator<String> it = params.iterator();
		while (it.hasNext()) {
			String tmpkey = it.next();
			String val = Coordinator.commonProps.get(tmpkey);
			if (tmpkey.equals(CoordinatorConstants.WORKLOADFILE_PROP) || tmpkey.equals(CoordinatorConstants.THREADCOUNT_PROP)
					|| tmpkey.equals(CoordinatorConstants.POPULATEFILE_PROP)
					|| tmpkey.equals(CoordinatorConstants.EXEPARAM_PROP)
					|| tmpkey.equals(CoordinatorConstants.EXETIME_PROP))
				continue;
			else if (tmpkey.equals(CoordinatorConstants.DB_PROP)) {
				loadMsg += " -db " + val;
			} else {
				loadMsg += " -p " + tmpkey + "=" + val;
			}
		}

		// add the load specific properties 
		params = Coordinator.loadProps.keySet();
		it = params.iterator();
		while (it.hasNext()) {
			String tmpkey = (String) it.next();
			String val = Coordinator.loadProps.get(tmpkey);
			if (tmpkey.equals(CoordinatorConstants.DB_PROP))
				loadMsg += " -db " + val;
			else
				loadMsg += " -p " + tmpkey + "=" + val;
		}

		loadMsg += " -p "+CoordinatorConstants.USERCOUNT_PROP+"=" + usercount + " -p "+CoordinatorConstants.PROBABILITIES_PROP+"=" + get_perc_string()
				+ " -p "+CoordinatorConstants.NUMCLIENTS_PROP+"=" + Coordinator.numBGClients
				+ " -p "+CoordinatorConstants.THREADCOUNT_PROP+"=" + Coordinator.numLoadThreads
				+ " -p "+CoordinatorConstants.USEROFFSET_PROP+"=" + useroffset + CoordinatorConstants.ENDOFMSG_IDENTIFIER;

		return loadMsg;
	}

	/**
	 * Creates the message that enables a BGClient to issue a workload against the data store
	 * @param threadid, BGClient's machine id
	 * @param threadcount, number of threads emulating workload against the data store by this BGClient
	 * @param usercount, total number of users in the social graph
	 * @param useroffset
	 * @param perc, percentage of work imposed by this BGClient
	 * @return
	 */
	public static String createBGThreadSimMsg(int threadid, int threadcount,
			String usercount, String useroffset) {

		String simMsg = "";
		simMsg = Coordinator.commonProps.get(CoordinatorConstants.EXEPARAM_PROP) + " "
				+ Coordinator.commonProps.get(CoordinatorConstants.BGMODE_PROP) + " -t " + " -P "
				+ Coordinator.commonProps.get(CoordinatorConstants.WORKLOADFILE_PROP)
				+ " -s -p "+CoordinatorConstants.MACHINEID_PROP+"=" + threadid + " -p "+CoordinatorConstants.THREADCOUNT_PROP+"="
				+ threadcount ;

		// add the common properties needed by the BGClient
		Set<String> params = Coordinator.commonProps.keySet();
		Iterator<String> it = params.iterator();
		while (it.hasNext()) {
			String tmpkey = it.next();
			String val = Coordinator.commonProps.get(tmpkey);
			if(tmpkey.equals("monitor"))
			System.out.println("%%%%%%%%%%%%%%%%%%%%"+val);
			if (tmpkey.equals(CoordinatorConstants.WORKLOADFILE_PROP) || tmpkey.equals(CoordinatorConstants.THREADCOUNT_PROP)
					|| tmpkey.equals(CoordinatorConstants.POPULATEFILE_PROP)
					|| tmpkey.equals(CoordinatorConstants.LOADEXEPARAM_PROP) || tmpkey.equals(CoordinatorConstants.EXEPARAM_PROP)
					|| tmpkey.equals(CoordinatorConstants.EXETIME_PROP))
				continue;
			else if (tmpkey.equals(CoordinatorConstants.DB_PROP))
				simMsg += " -db " + val;
			else {
				if (tmpkey.equals(CoordinatorConstants.RATINGUNIT_PROP)){
					tmpkey = CoordinatorConstants.BGEXEDURATION_PROP;
					val = Integer.toString((int) Coordinator.delta);
				}
				simMsg += " -p " + tmpkey + "=" + val;
			}
		}

		// add benchmarking phase specific properties
		params = Coordinator.benchmarkProps.keySet();
		it = params.iterator();
		while (it.hasNext()) {
			String tmpkey = it.next();
			String val = Coordinator.benchmarkProps.get(tmpkey);
			if (tmpkey.equals(CoordinatorConstants.DB_PROP))
				simMsg += " -db " + val;
			else {
				simMsg += " -p " + tmpkey + "=" + val;
			}
		}

		simMsg += " -p "+CoordinatorConstants.RATINGMODE_PROP+"=true -p "+CoordinatorConstants.USERCOUNT_PROP+"=" + usercount
				+ " -p "+CoordinatorConstants.PROBABILITIES_PROP+"=" + get_perc_string() + " -p "+CoordinatorConstants.NUMCLIENTS_PROP+"="
				+ Coordinator.numBGClients + " -p "+CoordinatorConstants.USEROFFSET_PROP+"=" + useroffset
				+ CoordinatorConstants.ENDOFMSG_IDENTIFIER;

		return simMsg;
	}


	public static void createSchemaAndLoad(Logger logger) {

		String dealWithSchemaCmd = createSchemaCommand();
		System.out.println("Creating schema for the datastore using the following command:"
				+ dealWithSchemaCmd);

		Process _schemaProcess;
		String BGline = "";
		try {
			//start only one BGClient on the node hosting the coordinator to create the schema
			_schemaProcess = Runtime.getRuntime().exec(dealWithSchemaCmd);
			InputStream stdout = _schemaProcess.getInputStream();
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					stdout));
			while ((BGline = reader.readLine()) != null) {
				System.out.println("Stdout: " + BGline);
			}
			// wait for the schema creation to complete
			_schemaProcess.waitFor();
			Thread.sleep(1000);
		} catch (IOException e2) {
			e2.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		// create the load messages to send to each BGClient
		String token = "-loadindex";
		Vector<BGLoadThread> loadThreads = new Vector<BGLoadThread>();
		for (int i = 0; i < Coordinator.numBGClients; i++) {
			BGLoadThread loader = new BGLoadThread(i, Coordinator.clientInfo.get(i),
					logger, token);
			loadThreads.add(loader);
			loader.start();
		}

		// wait for the load by all BGClient to complete
		for (int i = 0; i < loadThreads.size(); i++)
			try {
				loadThreads.get(i).join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		System.out.println("The population of the social graph by all the BGClients is completed.");
	}



	/*
	 * Helper functions
	 */
	public static String get_perc_string(){
		String probs = "";
		double fraction = 1.0/Coordinator.numBGClients;
		for (int i = 0; i < Coordinator.numBGClients; i++)
			probs += fraction + "@";
		return probs;
	}
}
