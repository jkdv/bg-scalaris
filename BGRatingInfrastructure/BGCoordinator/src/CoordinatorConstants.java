/**                                                                                                                                                                                
 * Copyright (c) 2014 USC Database Laboratory All rights reserved. 
 *
 * Authors:  Sumita Barahmand and Shahram Ghandeharizadeh                                                                                                                            
 *              http://bgbenchmark.org/BG/coordinator.html                                                                                                                                                                   
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

public class CoordinatorConstants {
	static final String EXEPARAM_PROP = "exe";
	static final String LOADEXEPARAM_PROP = "loadexe";
	static final String POPULATEFILE_PROP = "loadworkloadfile";
	static final String WORKLOADFILE_PROP = "workloadfile";
	static final String MACHINEID_PROP = "machineid";
	static final String RATINGMODE_PROP = "ratingmode";
	static final String THREADCOUNT_PROP = "threadcount";
	static final String LOADTHREADCOUNT_PROP = "numloadthreads";
	static final String EXETIME_PROP = "finalexecutiontime";
	static final String BGEXEDURATION_PROP = "maxexecutiontime";
	static final String RATINGUNIT_PROP ="ratingunit";
	static final String BGMODE_PROP = "bgmode";
	static final String PROBABILITIES_PROP = "probs";
	static final String NUMCLIENTS_PROP = "numclients";
	static final String DB_PROP = "datastore";
	static final String USERCOUNT_PROP = "usercount";
	static final String USEROFFSET_PROP = "useroffset";
	static final String BGLISTENERPORT_PROP = "port";
	static final String BGLISTENERIP_PROP = "ip";
	static final String LOADBETWEENROUNDS_PROP = "loadbetweenrounds";
	static final String RATINGTYPE_PROP = "ratingtype";
	static final String MONITOR_PROP = "monitor";
	static final String SLAEXPECTEDLATENCY_PROP ="expectedlatency";
	static final String SLAEXPECTEDCONFIDENCE_PROP ="expectedconfidence";
	static final String SLAEXPECTEDSTALENESS_PROP = "expectedstaleness";
	static final String ENDOFMSG_IDENTIFIER = " # ";
	static final String LOADINGCCOMPLETEMSG = "LoadingCompleted";
	static final String BGCLIENTINITIALIZEDMSG = "Connected";
	static final String BGSTARTSIMULATIONMSG = "StartSimulation ";
	static final String BGSHUTDOWNMSG = "shutdown BGClient ";
	static final String CLIENTINFOTOKEN = "#clientinfo";
	static final String SCHEMATOKEN = "schema:";
	static final String LOADTOKEN = "load:";
	static final String BENCHMARKTOKEN = "benchmark:";
	static final boolean closedSimulation = true;
	static final int ACTUALLOAD = 101;
	static final int numAttepmts = 10;
	static final String TIME_SEPARATOR = ",";
	static final int SOCIALITE_RATING = 0;
	static final int PRIM_SOAR_RATING = 1;
	static final int MAXTHREADS = 1024 * 1024;
	static final int GENERIC = 190;
	static final int CREATESCHEMANDLOAD = 91;
	static final int LOADMODE = ACTUALLOAD;
	
	
	
}
