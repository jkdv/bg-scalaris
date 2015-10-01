/**                                                                                                                                                                                
 * Copyright (c) 2014 USC Database Laboratory All rights reserved. 
 *
 * Authors:  Sumita Barahmand and Shahram Ghandeharizadeh                                                                                                                            
 *                http://bgbenchmark.org/BG/coordinator.html                                                                                                                                                                 
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
import java.net.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;


/**
 * Starts the BGClients which then do the warmup phase if required
 */
class BGStarterThread extends Thread {
	BGRaterThread _t = null;

	BGStarterThread(BGRaterThread t) {
		_t = t;
	}

	public void run() {
		try {
			_t.initThread();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}


/**
 * one thread per BG Client, responsible for constructing the load message for its BGClient and initiating
 * the process of loading by its BGClient
 * 
 * @author Sumita Barahmand
 * 
 */
class BGLoadThread extends Thread {
	/*
	 * Maintains the information for each BGClient
	 */
	HashMap<String, String> _clientInfo;
	/*
	 * Maintains the machine id for the BGClient
	 */
	int _threadid;
	/*
	 * Identified the port on which the BGClient's listener is listening on
	 */
	int _port;
	Process _process;
	Socket requestSocket = null;
	OutputStream out = null;
	InputStream in = null;
	Logger _logger = null;
	String _loadMsg = "";
	String _usercount = "";
	String _useroffset = "";
	String _loadToken;

	public BGLoadThread(int threadid, HashMap<String, String> clientInfo,
			Logger logger, String loadToken) {
		_threadid = threadid;
		_clientInfo = clientInfo;
		_port = Integer.parseInt(_clientInfo.get(CoordinatorConstants.BGLISTENERPORT_PROP));
		_logger = logger;
		_usercount = _clientInfo.get(CoordinatorConstants.USERCOUNT_PROP);
		_useroffset = _clientInfo.get(CoordinatorConstants.USEROFFSET_PROP);
		_loadToken = loadToken;
	}

	public void run() {
		_loadMsg = BGMessageCreationClass.createBGThreadLoadMsg(_threadid, _usercount, _useroffset);
		System.out
		.println("Thread "
				+ _threadid
				+ ": trying to connect to the BG client's Listener port for loading on port: "
				+ _port+".");
		try {
			requestSocket = new Socket(_clientInfo.get(CoordinatorConstants.BGLISTENERIP_PROP), _port);
			System.out.println("Thread " + _threadid
					+ ": connection socket created to BG client's listener for machine "
					+ _clientInfo.get(CoordinatorConstants.BGLISTENERIP_PROP) + " on port " + _port + ".");
			out = requestSocket.getOutputStream();
			in = requestSocket.getInputStream();
			PrintWriter outp = new PrintWriter(out);
			Scanner ins = new Scanner(in);
			System.out.println("Thread " + _threadid
					+ ": sending load msg to BG client's listener... MSG:"
					+ _loadMsg +" .");

			// send the load message to the BGClient
			outp.print(_loadMsg);
			outp.flush();
			System.out.println("Thread " + _threadid + ": sent the " + _loadMsg
					+ " msg to the BG client.");

			String msg = "";
			//wait till the BGClient completes loading its own fragment of the social graph
			while (ins.hasNext()) {
				msg = ins.next();
				if (msg.equalsIgnoreCase(CoordinatorConstants.LOADINGCCOMPLETEMSG)) {
					System.out.println(_clientInfo.get(CoordinatorConstants.BGLISTENERIP_PROP) + "****Thread "
							+ _threadid + ": Loading completed.");
					break;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			try {
				if (in != null)
					in.close();
				if (out != null)
					out.close();
				if (requestSocket != null)
					requestSocket.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			if (_process != null)
				_process.destroy();
		}
	}

}

/**
 * one thread per BG Client, responsible for creating the simulation message for its BGClient and communicating the message to it
 * to issue a workload against the data store
 * 
 * @author Sumita Barahmand
 * 
 */
class BGRaterThread extends Thread {
	HashMap<String, String> _commons;
	HashMap<String, String> _clientInfo;
	/*
	 * The BGClient's machine id
	 */
	int _threadid;
	/*
	 * Number of threads used to emulate actions against the data store by this BGClient
	 */
	int _threadcount;
	/*
	 * Port on which the BGListener for the BGClient is listening on
	 */
	int _port;
	/*
	 * Total number of attempted sessions
	 */
	double sessionCount = 0;
	/*
	 * Total number of issued actions
	 */
	double actionCount = 0;
	/*
	 * Percentage of stale data observed from the data store by this BGClient
	 */
	double stalenessPerc = 0;
	/*
	 * Percentage of actions issued by this BGClient satisfying the average response time specified by the SLA
	 */
	double confidencePerc = 0;
	/*
	 * Identifies if this BGClient satisfies the SLA requirements
	 */
	boolean _succeeded = false;
	double throughput = 0;
	double actthroughput = 0;
	double simDuration = 0;
	double rampedThroughput = 0;
	double rampedActThroughput = 0;
	double prevactthroughput = 0;
	double rampedSimDuration = 0;
	double rampedSessionCount = 0;
	double rampedActionCount = 0;
	Process _process;
	String _simMsg = "";
	Socket requestSocket = null;
	OutputStream out = null;
	InputStream in = null;
	HashMap<String, Boolean> _timeToKill = null;
	Logger _logger = null;
	String _usercount = "";
	String _useroffset = "";
	int _monitoringRound = 0;
	boolean _finalRound = false;


	public BGRaterThread(int threadid, HashMap<String, String> commonProps,
			HashMap<String, String> clientInfo, int threadcount,
			HashMap<String, Boolean> timeToKill, Logger logger,
			boolean finalRound) {
		_threadid = threadid;
		_clientInfo = clientInfo;
		_commons = commonProps;
		_threadcount = threadcount;
		_port = Integer.parseInt(_clientInfo.get("port"));
		_timeToKill = timeToKill;
		_logger = logger;
		_usercount = _clientInfo.get("usercount");
		_useroffset = _clientInfo.get("useroffset");
		_finalRound = finalRound;
	}

	public boolean isFinalRound() {
		return _finalRound;
	}

	public int getThreadCount() {
		return _threadcount;
	}

	public double getStalenessPerc() {
		return stalenessPerc;
	}

	public double getThroughput() {
		return throughput;
	}

	public double getActThroughput() {
		return actthroughput;
	}

	public double getSimDuration() {
		return simDuration;
	}

	public double getRampedThroughput() {
		return rampedThroughput;
	}

	public double getRampedActThroughput() {
		return rampedActThroughput;
	}

	public double getRampedSimDuration() {
		return rampedSimDuration;
	}

	public double getRampedSessionCount() {
		return rampedSessionCount;
	}

	public double getRampedActionCount() {
		return rampedActionCount;
	}

	public double getSessionCount() {
		return sessionCount;
	}

	public double getActionCount() {
		return actionCount;
	}

	public int getMonitoringRound() {
		return _monitoringRound;
	}

	public double getConfidence() {
		return confidencePerc;
	}

	public boolean succeeded() {
		if (confidencePerc >= Double.parseDouble(_commons
				.get(CoordinatorConstants.SLAEXPECTEDCONFIDENCE_PROP))
				&& stalenessPerc <= Double.parseDouble(_commons
						.get(CoordinatorConstants.SLAEXPECTEDSTALENESS_PROP))) {
			System.out.println("succeeded!");
			return true;
		} else {
			return false;
		}
	}

	/*
	 * Initializes the BGClient
	 */
	public void initThread() throws IOException {
		_simMsg = BGMessageCreationClass.createBGThreadSimMsg(_threadid, _threadcount, _usercount,
				_useroffset);
		System.out.println("Thread " + _threadid
				+ ": trying to connect to the BG client's Listener port "
				+ _port+ " .");
		try {
			requestSocket = new Socket(_clientInfo.get(CoordinatorConstants.BGLISTENERIP_PROP), _port);
			System.out.println("Thread " + _threadid
					+ " connection socket created to BG client's listener "
					+ _clientInfo.get(CoordinatorConstants.BGLISTENERIP_PROP) + " on port " + _port +" .");
			out = requestSocket.getOutputStream();
			in = requestSocket.getInputStream();
			PrintWriter outp = new PrintWriter(out);
			Scanner ins = new Scanner(in);
			System.out.println("Thread " + _threadid
					+ ": sending msg to BG client's listener... " + _simMsg +" .");

			// sending the simulation message to the BGClient
			outp.print(_simMsg);
			outp.flush();
			System.out.println("Thread " + _threadid + ": sent the " + _simMsg
					+ " msg to the BG client.");

			//Waiti for the BGClient to get initialized
			String msg = "";
			while (ins.hasNext()) {
				msg = ins.next();
				if (msg.equalsIgnoreCase(CoordinatorConstants.BGCLIENTINITIALIZEDMSG)) {
					System.out
					.println(_clientInfo.get(CoordinatorConstants.BGLISTENERIP_PROP)
							+ "****Thread "
							+ _threadid
							+ ": the connection is established and the BGClient Listener has started the BG client.");
					break;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			if (in != null)
				in.close();
			if (out != null)
				out.close();
			if (requestSocket != null)
				requestSocket.close();
			if (_process != null)
				_process.destroy();
			System.exit(0);
		}
	}

	/*
	 * Run the simulation and issue workload against the data store
	 */

	public void run() {
		// send a start message to BGClient so the client will start the
		// simulation
		// wait to get the return message with all statistics
		// validate the statistics
		try {
			Scanner ins = new Scanner(in);
			PrintWriter outp = new PrintWriter(out);
			System.out
			.println("Thread "
					+ _threadid
					+ ": sending start message to the BG client Listener which forwards it to the BG client...");

			// send the start simulation message to the BGClient
			outp.print(CoordinatorConstants.BGSTARTSIMULATIONMSG);
			outp.flush();
			System.out
			.println("Thread "
					+ _threadid
					+ ": sent the "
					+ CoordinatorConstants.BGSTARTSIMULATIONMSG
					+ " msg to the BG client Listener which will fwd it to the BG client...");
			System.out.println("Thread " + _threadid
					+ ": waiting to get the stats...");

			// now keep on reading from the socket till you get final output
			boolean gotThru = false, gotLatency = false, gotActThru = false;
			while (ins.hasNext() && !_timeToKill.get("kill")) {
				if (_timeToKill.get("kill")) {
					outp.print("KILL ");
					outp.flush();
					System.out.println(_clientInfo.get("ip") + ":"
							+ _clientInfo.get("port") + " Thread " + _threadid
							+ "  sent kill msg to its BG listener shell");
				}
				if (gotLatency && gotThru && gotActThru) {
					_monitoringRound++;
					System.out.println("Doing monitoring validation...");
					String line = "Monitoring:" + _clientInfo.get("ip") + ":"
							+ _clientInfo.get("port")
							+ ",LatencyConfidenceGained:" + confidencePerc
							+ " ,ActThroughput:" + actthroughput;
					/*if (!succeeded() && _finalRound) {
						line += ", false";
						_timeToKill.put("kill", true);
					} else*/ {
						line += ", true";
					}
					gotThru = false;
					gotLatency = false;
					gotActThru = false;
					_logger.write(line);
				}
				String msgrec = ins.next();
				System.out.println(_clientInfo.get(CoordinatorConstants.BGLISTENERIP_PROP) + ":"
						+ _clientInfo.get("port") + " ***Thread " + _threadid
						+ ": " + msgrec);
				if (msgrec.contains("MONITOR-THROUGHPUT(SESSIONS/SEC):")) {
					gotThru = true;
					throughput = Double.parseDouble(msgrec.substring(msgrec
							.indexOf(":") + 1));
				} else if (msgrec.contains("MONITOR-THROUGHPUT(ACTIONS/SEC):")) {
					gotActThru = true;
					prevactthroughput = actthroughput;
					actthroughput = Double.parseDouble(msgrec.substring(msgrec
							.indexOf(":") + 1));
				} else if (msgrec.contains("MONITOR-SATISFYINGOPS(%):")) {
					gotLatency = true;
					confidencePerc = Double.parseDouble(msgrec.substring(msgrec
							.indexOf(":") + 1));
				} else if (msgrec.contains("OVERALLOPCOUNT(SESSIONS)"))
					sessionCount = Double.parseDouble(msgrec.substring(msgrec
							.indexOf(":") + 1));
				else if (msgrec.contains("OVERALLOPCOUNT(ACTIONS)"))
					actionCount = Double.parseDouble(msgrec.substring(msgrec
							.indexOf(":") + 1));
				else if (msgrec.contains("OVERALLRUNTIME(ms)"))
					simDuration = Double.parseDouble(msgrec.substring(msgrec
							.indexOf(":") + 1));
				else if (msgrec.contains("OVERALLTHROUGHPUT(SESSIONS/SECS)"))
					throughput = Double.parseDouble(msgrec.substring(msgrec
							.indexOf(":") + 1));
				else if (msgrec.contains("OVERALLTHROUGHPUT(ACTIONS/SECS)"))
					actthroughput = Double.parseDouble(msgrec.substring(msgrec
							.indexOf(":") + 1));
				else if (msgrec.contains("RAMPEDRUNTIME(ms)"))
					rampedSimDuration = Double.parseDouble(msgrec
							.substring(msgrec.indexOf(":") + 1));
				else if (msgrec.contains("RAMPEDOPCOUNT(SESSIONS)"))
					rampedSessionCount = Double.parseDouble(msgrec
							.substring(msgrec.indexOf(":") + 1));
				else if (msgrec.contains("RAMPEDOPCOUNT(ACTIONS)"))
					rampedActionCount = Double.parseDouble(msgrec
							.substring(msgrec.indexOf(":") + 1));
				else if (msgrec.contains("RAMPEDRUNTIME(ms)"))
					rampedSimDuration = Double.parseDouble(msgrec
							.substring(msgrec.indexOf(":") + 1));
				else if (msgrec.contains("RAMPEDTHROUGHPUT(SESSIONS/SECS)"))
					rampedThroughput = Double.parseDouble(msgrec
							.substring(msgrec.indexOf(":") + 1));
				else if (msgrec.contains("RAMPEDTHROUGHPUT(ACTIONS/SECS)"))
					rampedActThroughput = Double.parseDouble(msgrec
							.substring(msgrec.indexOf(":") + 1));
				else if (msgrec.contains("STALENESS(OPS)"))
					stalenessPerc = Double.parseDouble(msgrec.substring(msgrec
							.indexOf(":") + 1));
				else if (msgrec.contains("SATISFYINGOPS(%)"))
					confidencePerc = Double.parseDouble(msgrec.substring(msgrec
							.indexOf(":") + 1));
			}

			/*if (_timeToKill.get("kill")) {
				outp.print("KILL ");
				outp.flush();
				System.out.println("Thread " + _threadid
						+ "  sent kill msg to its BG listener shell");
			}*/
			System.out.println(_threadid + "Experiment done!");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (in != null)
				try {
					in.close();
					if (out != null)
						out.close();
					if (requestSocket != null)
						requestSocket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			if (_process != null)
				_process.destroy();
		}
	}
}

/**
 * The main coordinator class responsible for creating data store schema, load
 * BGClients and benchmarking BGClients it uses the information gathered from
 * every round to do Socialite rating, init SOAR rating or SOAR rating
 * 
 * @author Sumita Barahmand
 * 
 */

public class Coordinator {

	static int ratingType = 0;
	static int numBGClients = 1;
	static int initialThreadCount = 1;
	static boolean doLoad = false;
	static HashMap<String, Boolean> timeToKill = new HashMap<String, Boolean>();
	static int delta = 0;
	static int monitor = 0;
	static int numLoadThreads = 10;
	static int numLoadRoundSoFar = 0;

	// for the parameters from the config file
	static HashMap<String, String> commonProps = new HashMap<String, String>();
	static HashMap<String, String> schemaProps = new HashMap<String, String>();
	static HashMap<String, String> loadProps = new HashMap<String, String>();
	static HashMap<String, String> benchmarkProps = new HashMap<String, String>();
	static Vector<HashMap<String, String>> clientInfo = new Vector<HashMap<String, String>>();


	public static void main(String[] args) throws IOException {

		int finalThreadCount = 1;
		int currThreadCount = initialThreadCount; 
		boolean ratingDone = false;
		String token = "Socialites";
		double maxThroughput = 0;
		timeToKill.put("kill", false);
		HashMap<String, String> currPrintStats = new HashMap<String, String>();
		boolean finalRoundRating = false;
		int failRound = 0;
		int maxMonitorRound = 0;
		int duration = 0;

		// parameters needed for socialites rating
		int maxSatisfiedTillNow = 1;
		int minNotSatisfiedTillNow = 1;

		// parameters needed for SOAR rating
		int minThreadCount = 1;
		int prevPlusThreadCount = minThreadCount;
		int prevNegThreadCount = -1;
		int finalSuccess = minThreadCount;
		int finalFail = minThreadCount;
		
		if (args.length < 1) {
			System.out
			.println("The config file is missing.");
			System.exit(0);
		}

		String BGconfigFile = args[0];
		String logFile = "log.txt";

		//Read the configuration file
		readParams(BGconfigFile);
		delta = Integer.parseInt(commonProps.get(CoordinatorConstants.RATINGUNIT_PROP));
		currThreadCount = initialThreadCount;

		//prepare output files
		String benchmarkDataStore = commonProps.get(CoordinatorConstants.DB_PROP);
		if (benchmarkProps.get(CoordinatorConstants.DB_PROP) != null)
			benchmarkDataStore = benchmarkProps.get(CoordinatorConstants.DB_PROP);
		Logger logger = new Logger(logFile, false, true);
		Logger highLevelLogger = new Logger("FinalResults" + benchmarkDataStore
				+ ".txt", true, true);
		highLevelLogger.write(CoordinatorConstants.TIME_SEPARATOR + " BGconfigFile, " + BGconfigFile);
		highLevelLogger
		.write(CoordinatorConstants.TIME_SEPARATOR
				+ "time ,Objective, SLA Latency, SLA Confidence, numClients, ThreadCount, Throughput, ActThroughput, totalStaleness,  TotalClientsSucceeded");
		Logger UIGraph = new Logger("Results.txt", true, false);
		String currTable = "CurrentStats.txt";



		if (ratingType == CoordinatorConstants.SOCIALITE_RATING)
			token = "Socialites";
		else if (ratingType == CoordinatorConstants.PRIM_SOAR_RATING)
			token = "InitialSOAR";

		// construct the social graph once for the read only workloads
		if (!doLoad) {
			createSocialGraph(logger, numLoadRoundSoFar);
			numLoadRoundSoFar++;
		}

		if (ratingType == CoordinatorConstants.PRIM_SOAR_RATING) {
			if (doLoad) {
				createSocialGraph(logger, numLoadRoundSoFar);
				numLoadRoundSoFar++;
			}
			commonProps.put(CoordinatorConstants.RATINGUNIT_PROP, Integer.toString(delta));
			commonProps.put(CoordinatorConstants.MONITOR_PROP, Integer.toString(monitor));
			currThreadCount = initialThreadCount;
			int warmupround = 1;
			RatingResult rr = null;
			for (int j = 0; j < warmupround; j++) {
				long start = System.currentTimeMillis();
				rr = runBenchmark(currThreadCount, commonProps,
						clientInfo, logger, currPrintStats, UIGraph,
						maxMonitorRound, false, true);
				long end = System.currentTimeMillis();
				highLevelLogger
				.write(CoordinatorConstants.TIME_SEPARATOR + (end - start) + "," + token
						+ "," + commonProps.get(CoordinatorConstants.SLAEXPECTEDLATENCY_PROP)
						+ "," + commonProps.get(CoordinatorConstants.SLAEXPECTEDCONFIDENCE_PROP)
						+ "," + clientInfo.size() + ","
						+ currThreadCount + ","
						+ rr.get_totalThroughput() + ","
						+ rr.get_totalActThroughput() + ","
						+ rr.get_totalStaleness() + ","
						+ rr.get_numSucceeded());
			}
			if (rr.get_numSucceeded() == numBGClients) {
				System.out
				.println("####### First Round Succeeded! total threads="
						+ currThreadCount);
				maxThroughput = rr.get_totalActThroughput();
				currThreadCount = currThreadCount * 2;
			} else {
				System.out.println("####### First round with " + currThreadCount
						+ " threads did not succeed!");
				System.exit(0);
			}
		} 

		while (true) {
			RatingResult rr = null;
			long start = 0, end = 0;

			// for workloads that involve updates, the social graph needs to be populated before each experiment
			if (doLoad) {
				createSocialGraph(logger, numLoadRoundSoFar);
				numLoadRoundSoFar++;
			}
			
			// if the rating is completed, final round with the duration specified in SLA needs to be executed
			if (ratingDone) {
				if (finalRoundRating) {
					// Running an experiment with the thread count computed by the rating heuristic
					// and the duration specified by SLA does not satisfy the SLA requirements so backing off
					System.out.println("backing off:" + duration + " "
							+ maxMonitorRound);
					commonProps.put(CoordinatorConstants.RATINGUNIT_PROP,
							Integer.toString(duration));
				} else {
					System.out.println("FinalActualRound:"
							+ commonProps.get(CoordinatorConstants.EXETIME_PROP));
					commonProps.put(CoordinatorConstants.RATINGUNIT_PROP,
							commonProps.get(CoordinatorConstants.EXETIME_PROP));
				}
				commonProps.put(CoordinatorConstants.MONITOR_PROP, Integer.toString(monitor));
			} else {
				// normal rating round
				commonProps.put(CoordinatorConstants.RATINGUNIT_PROP, Integer.toString(delta));
				commonProps.put(CoordinatorConstants.MONITOR_PROP, Integer.toString(monitor));
			}

			start = System.currentTimeMillis();
			rr = runBenchmark(currThreadCount, commonProps,
					clientInfo, logger, currPrintStats, UIGraph,
					maxMonitorRound, ratingDone, true);
			maxMonitorRound = rr.get_maxMonitoringRound();
			end = System.currentTimeMillis();

			if (ratingDone) { 
				if (numBGClients == rr.get_numSucceeded()) {
					logger.write("Final round with " + currThreadCount
							+ " succeeded.");
					finalThreadCount = currThreadCount;
					if (ratingType == CoordinatorConstants.SOCIALITE_RATING) {
						logger.write("Rating Type: SOCIALITE RATING");
						System.out.println("Max Decided ThreadCount:"
								+ finalThreadCount);
						logger.write("Max Decided ThreadCount:"
								+ finalThreadCount);
						System.out
						.println("Max Decided ThreadCounts - Throughput(sessions) :"
								+ rr.get_totalThroughput());
						logger.write("Max Decided ThreadCounts -  Throughput(sessions) :"
								+ rr.get_totalThroughput());
						System.out
						.println("Max Decided ThreadCounts - Throughput(actions) :"
								+ rr.get_totalActThroughput());
						logger.write("Max Decided ThreadCounts -  Throughput(actions) :"
								+ rr.get_totalActThroughput());
					} else if (ratingType == CoordinatorConstants.PRIM_SOAR_RATING) {
						logger.write("Rating Type: SOAR RATING");
						System.out.println("Max Decided ThreadCount:"
								+ finalThreadCount);
						logger.write("Max Decided ThreadCount:"
								+ finalThreadCount);
						System.out
						.println("Max Decided ThreadCounts - Throughput(sessions) :"
								+ rr.get_totalThroughput());
						logger.write("Max Decided ThreadCounts -  Throughput(sessions) :"
								+ rr.get_totalThroughput());
						System.out
						.println("Max Decided ThreadCounts - Throughput(actions) :"
								+ rr.get_totalActThroughput());
						logger.write("Max Decided ThreadCounts -  Throughput(actions) :"
								+ rr.get_totalActThroughput());
					}
					failRound = 0;
					finalSuccess = currThreadCount;
					end = System.currentTimeMillis();
					highLevelLogger.write(CoordinatorConstants.TIME_SEPARATOR + "LastRound,"
							+ token + ","
							+ commonProps.get(CoordinatorConstants.SLAEXPECTEDLATENCY_PROP) + ","
							+ commonProps.get(CoordinatorConstants.SLAEXPECTEDCONFIDENCE_PROP) + ","
							+ clientInfo.size() + "," + currThreadCount
							+ "," + rr.get_totalThroughput() + ", "
							+ rr.get_totalActThroughput() + ","
							+ rr.get_totalStaleness() + ","
							+ rr.get_numSucceeded());
					if (finalRoundRating) {
						int interval = finalFail - finalSuccess;
						if (interval < 2) {
							// run for complete duration with the same threadcount
							finalRoundRating = false;
							duration = Integer.parseInt(commonProps
									.get(CoordinatorConstants.EXETIME_PROP));
							continue;
						} else {
							currThreadCount = finalSuccess + (interval / 2);
							duration = maxMonitorRound * monitor;
							continue;
						}
					}
					duration = Integer.parseInt(commonProps
							.get(CoordinatorConstants.EXETIME_PROP));
					break;
				} else {
					// the final round did not succeed so the rating is invalid
					logger.write("Final round with " + currThreadCount
							+ " did not succeed, need to do rating again.");
					finalRoundRating = true;
					finalFail = currThreadCount;
					// starting to go one backwards similar to a quad scan
					if (currThreadCount > minThreadCount) {
						int red = (int) (Math.pow(2, failRound));
						failRound++;
						if ((currThreadCount - red) > minThreadCount)
							currThreadCount = currThreadCount - red;
						else
							currThreadCount = currThreadCount - 1;
						duration = Integer.parseInt(commonProps
								.get(CoordinatorConstants.EXETIME_PROP));
						continue;
					} else {
						logger.write("Final round has no more options to go backwards so exiting with failure.");
						end = System.currentTimeMillis();
						highLevelLogger.write(CoordinatorConstants.TIME_SEPARATOR + "LastRound,"
								+ token + ","
								+ commonProps.get(CoordinatorConstants.SLAEXPECTEDLATENCY_PROP) + ","
								+ commonProps.get(CoordinatorConstants.SLAEXPECTEDCONFIDENCE_PROP)
								+ "," + clientInfo.size() + ","
								+ currThreadCount + ","
								+ rr.get_totalThroughput() + ", "
								+ rr.get_totalActThroughput() + ","
								+ rr.get_totalStaleness() + ","
								+ rr.get_numSucceeded());
						duration = Integer.parseInt(commonProps
								.get(CoordinatorConstants.EXETIME_PROP));
						break;
					}
				}
			}

			if (!ratingDone) {
				if (ratingType == CoordinatorConstants.SOCIALITE_RATING) {
					String line = "," + token + ","
							+ commonProps.get(CoordinatorConstants.SLAEXPECTEDLATENCY_PROP) + ","
							+ commonProps.get(CoordinatorConstants.SLAEXPECTEDCONFIDENCE_PROP) + ","
							+ clientInfo.size() + "," + currThreadCount + ","
							+ rr.get_totalThroughput() + ","
							+ rr.get_totalActThroughput() + ","
							+ rr.get_totalStaleness() + ","
							+ rr.get_numSucceeded();
					if (rr.get_numSucceeded() == numBGClients) {
						System.out
						.println("####### All succeeded! total threads="
								+ currThreadCount);
						maxSatisfiedTillNow = currThreadCount;
						if (minNotSatisfiedTillNow == 1) {
							if (currThreadCount == CoordinatorConstants.MAXTHREADS) {
								finalThreadCount = currThreadCount;
								ratingDone = true;
							} else if (currThreadCount * 2 < CoordinatorConstants.MAXTHREADS)
								currThreadCount = currThreadCount * 2;
							else
								currThreadCount = CoordinatorConstants.MAXTHREADS;
						} else {
							int interval = minNotSatisfiedTillNow
									- currThreadCount;
							if (interval < 2) {
								finalThreadCount = maxSatisfiedTillNow;
								currThreadCount = finalThreadCount;
								ratingDone = true;
							} else {
								int mid = interval / 2;
								currThreadCount = currThreadCount + mid;
							}
						}
					} else {
						minNotSatisfiedTillNow = currThreadCount;
						int interval = currThreadCount - maxSatisfiedTillNow;
						if (interval < 2) {
							finalThreadCount = maxSatisfiedTillNow;
							currThreadCount = finalThreadCount;
							ratingDone = true;
						} else {
							int mid = interval / 2;
							currThreadCount = maxSatisfiedTillNow + mid;
						}
					}
					end = System.currentTimeMillis();
					highLevelLogger
					.write(CoordinatorConstants.TIME_SEPARATOR + (end - start) + line);
				} else if (ratingType == CoordinatorConstants.PRIM_SOAR_RATING) {
					String line = "," + token + ","
							+ commonProps.get(CoordinatorConstants.SLAEXPECTEDLATENCY_PROP) + ","
							+ commonProps.get(CoordinatorConstants.SLAEXPECTEDCONFIDENCE_PROP) + ","
							+ clientInfo.size() + "," + currThreadCount + ","
							+ rr.get_totalThroughput() + ","
							+ rr.get_totalActThroughput() + ","
							+ rr.get_totalStaleness() + ","
							+ rr.get_numSucceeded();
					if (rr.get_numSucceeded() == numBGClients) {
						if (rr.get_totalActThroughput() >= maxThroughput) {
							prevPlusThreadCount = currThreadCount;
							if (prevNegThreadCount == -1) {
								if (currThreadCount == CoordinatorConstants.MAXTHREADS) {
									finalThreadCount = currThreadCount;
									ratingDone = true;
									
								} else if (currThreadCount * 2 < CoordinatorConstants.MAXTHREADS)
									currThreadCount = currThreadCount * 2;
								else
									currThreadCount = CoordinatorConstants.MAXTHREADS;
							} else {
								int interval = prevNegThreadCount
										- currThreadCount;
								if (interval < 2) {
									finalThreadCount = prevPlusThreadCount;
									currThreadCount = finalThreadCount;
									ratingDone = true;
								} else {
									int mid = interval / 2;
									currThreadCount = currThreadCount + mid;
								}
							}
							maxThroughput = rr.get_totalActThroughput();
						} else {
							prevNegThreadCount = currThreadCount;
							int interval = currThreadCount
									- prevPlusThreadCount;
							if (interval < 2) {
								finalThreadCount = prevPlusThreadCount;
								currThreadCount = finalThreadCount;
								ratingDone = true;
							} else {
								int mid = interval / 2;
								currThreadCount = prevPlusThreadCount + mid;
							}

						}

					} else {
						prevNegThreadCount = currThreadCount;
						int interval = currThreadCount - prevPlusThreadCount;
						if (interval < 2) {
							finalThreadCount = prevPlusThreadCount;
							currThreadCount = finalThreadCount;
							ratingDone = true;
						} else {
							int mid = interval / 2;
							currThreadCount = prevPlusThreadCount + mid;
						}

					}
					end = System.currentTimeMillis();
					highLevelLogger
					.write(CoordinatorConstants.TIME_SEPARATOR + (end - start) + line);

				}
			}

		}
		// Listeneres kill their BGClients
		sendShutdownBGClientMsgToListeners();
		logger.close();
		highLevelLogger.close();
		UIGraph.close();

	}

	/**
	 * Kills all BGClients at the end of a rating experiment
	 */
	public static void sendShutdownBGClientMsgToListeners() {
		Socket requestSocket = null;
		for (int i = 0; i < clientInfo.size(); i++) {
			try {
				requestSocket = new Socket(clientInfo.get(i).get(CoordinatorConstants.BGLISTENERIP_PROP),
						Integer.parseInt(clientInfo.get(i).get(CoordinatorConstants.BGLISTENERPORT_PROP)));
				System.out.println("Sending BGClient shutdown message to "
						+ clientInfo.get(i).get(CoordinatorConstants.BGLISTENERIP_PROP) + ":"
						+ clientInfo.get(i).get(CoordinatorConstants.BGLISTENERPORT_PROP));
				OutputStream out = requestSocket.getOutputStream();
				PrintWriter outp = new PrintWriter(out);
				outp.print(CoordinatorConstants.BGSHUTDOWNMSG);
				outp.flush();
				outp.close();
				Thread.sleep(5000);
			} catch (NumberFormatException e) {
				e.printStackTrace();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
				if (requestSocket != null) {
					try {
						requestSocket.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}

		}

	}

	public static void readParams(String fileName) {
		DataInputStream in = null;
		BufferedReader br = null;
		try {
			FileInputStream fstream = new FileInputStream(fileName);
			in = new DataInputStream(fstream);
			br = new BufferedReader(new InputStreamReader(in));
			String strLine = "";
			while ((strLine = br.readLine()) != null) {
				if (strLine.trim() == "")
					continue;
				if (strLine.contains(CoordinatorConstants.CLIENTINFOTOKEN)) {
					HashMap<String, String> client = new HashMap<String, String>();
					String tmp = br.readLine();
					String ip = tmp.substring(tmp.indexOf("=") + 1);
					client.put(CoordinatorConstants.BGLISTENERIP_PROP, ip);
					tmp = br.readLine();
					String port = tmp.substring(tmp.indexOf("=") + 1);
					client.put(CoordinatorConstants.BGLISTENERPORT_PROP, port);
					tmp = br.readLine();
					String count = tmp.substring(tmp.indexOf("=") + 1);
					client.put(CoordinatorConstants.USERCOUNT_PROP, count);
					tmp = br.readLine();
					String offset = tmp.substring(tmp.indexOf("=") + 1);
					client.put(CoordinatorConstants.USEROFFSET_PROP, offset);
					clientInfo.add(client);
				} else if (strLine.contains(CoordinatorConstants.THREADCOUNT_PROP)) {
					initialThreadCount = Integer.parseInt(strLine
							.substring(strLine.indexOf("=") + 1));
				} else if (strLine.contains(CoordinatorConstants.NUMCLIENTS_PROP)) {
					numBGClients = Integer.parseInt(strLine.substring(strLine
							.indexOf("=") + 1));
				} else if (strLine.contains(CoordinatorConstants.LOADBETWEENROUNDS_PROP)) {
					doLoad = Boolean.parseBoolean(strLine.substring(strLine
							.indexOf("=") + 1));
				} else if (strLine.contains(CoordinatorConstants.RATINGTYPE_PROP)) {
					ratingType = Integer.parseInt(strLine.substring(strLine
							.indexOf("=") + 1));
				} else if (strLine.contains(CoordinatorConstants.MONITOR_PROP)) {
					monitor = Integer.parseInt(strLine.substring(strLine
							.indexOf("=") + 1));
				}else {
					if (strLine.contains(CoordinatorConstants.LOADTHREADCOUNT_PROP))
						numLoadThreads = Integer.parseInt(strLine
								.substring(strLine.indexOf("=") + 1));
					if (strLine.contains(CoordinatorConstants.SCHEMATOKEN)) {
						strLine = strLine.substring(strLine.indexOf(":") + 1);
						schemaProps.put(
								strLine.substring(0, strLine.indexOf("=")),
								strLine.substring(strLine.indexOf("=") + 1));
					} else if (strLine.contains(CoordinatorConstants.LOADTOKEN)) {
						strLine = strLine.substring(strLine.indexOf(":") + 1);
						loadProps.put(
								strLine.substring(0, strLine.indexOf("=")),
								strLine.substring(strLine.indexOf("=") + 1));
					} else if (strLine.contains(CoordinatorConstants.BENCHMARKTOKEN)) {
						strLine = strLine.substring(strLine.indexOf(":") + 1);
						benchmarkProps.put(
								strLine.substring(0, strLine.indexOf("=")),
								strLine.substring(strLine.indexOf("=") + 1));
					} else {
						commonProps.put(
								strLine.substring(0, strLine.indexOf("=")),
								strLine.substring(strLine.indexOf("=") + 1));
					}
				}
			}
		} catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
		} finally {
			try {
				if (in != null)
					in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void doLoadData(Logger logger, int round) {
		try {
			System.out.println("Loading round " + round);
			long startTime = System.currentTimeMillis();
			BGMessageCreationClass.createSchemaAndLoad(logger);
			long endTime = System.currentTimeMillis();
			System.out.println("Loading time = " + (endTime - startTime));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void createSocialGraph(Logger logger,
			int numLoadRoundsSoFar) {
		doLoadData(logger, numLoadRoundSoFar);

	}

	public static String sendMessage(Socket requestSocket, String message,
			String opType) throws IOException {
		OutputStream out = requestSocket.getOutputStream();
		DataOutputStream os = new DataOutputStream(
				new BufferedOutputStream(out));
		os.writeBytes(opType + " ");
		os.writeInt(message.length());
		os.writeBytes(message);
		os.flush();

		DataInputStream is = new DataInputStream(new BufferedInputStream(
				requestSocket.getInputStream()));
		String line = "";
		String response = "0";
		BufferedReader bri = new BufferedReader(new InputStreamReader(is));
		// just wait for the output
		while ((line = bri.readLine()) != null)
			;
		is.close();
		os.close();
		out.close();
		requestSocket.close();
		return response;
	}


	/*
	 * Starts the rating experiment
	 */
	public static RatingResult runBenchmark(int currThreadCount,
			HashMap<String, String> commonProps,
			Vector<HashMap<String, String>> clientInfo, Logger logger,
			HashMap<String, String> currPrintStats, Logger UIGraph,
			int maxMonitorRound, boolean finalRound, boolean firstRound) {

		RatingResult rr = null;
		String[] logs = new String[numBGClients];
		Vector<BGRaterThread> BGRaters = new Vector<BGRaterThread>();
		int totalThreads = 0;
		String lineForHash = "";
		Vector<BGStarterThread> stThreads = new Vector<BGStarterThread>();

		// compute the number of threads for each BGClient based on previous run
		for (int i = 0; i < numBGClients; i++) {
			totalThreads += currThreadCount * (1.0/numBGClients);
		}
		int remainingThreads = currThreadCount - totalThreads;

		// start the BGClients and tell them which port to listen on for communication with the BGListener
		for (int i = 0; i < numBGClients; i++) {
			int ctCount = (int) (currThreadCount * (1.0/numBGClients) );
			if (i < remainingThreads)
				ctCount += 1;
			BGRaterThread BG = new BGRaterThread(i, commonProps,
					clientInfo.get(i), ctCount, timeToKill, logger, finalRound);
			BGRaters.add(BG);

			// Start the BGClient and do warm sup if needed
			BGStarterThread starterThread = new BGStarterThread(BG);
			stThreads.add(starterThread);
			starterThread.start();

			logs[i] = clientInfo.get(i).get(CoordinatorConstants.BGLISTENERIP_PROP) + ":"
					+ clientInfo.get(i).get(CoordinatorConstants.BGLISTENERPORT_PROP) + "," + (1.0/numBGClients) + ",";
			System.out.println("starting " + clientInfo.get(i).get(CoordinatorConstants.BGLISTENERIP_PROP) + " "
					+ clientInfo.get(i).get(CoordinatorConstants.BGLISTENERPORT_PROP) + " with " + ctCount);
			lineForHash = "\"Type\":\"" + clientInfo.get(i).get(CoordinatorConstants.BGLISTENERIP_PROP) + ":"
					+ clientInfo.get(i).get(CoordinatorConstants.BGLISTENERPORT_PROP) + "\", \"Threads\":"
					+ ctCount + ", \"WorkloadPercentage\":" + (1.0/numBGClients) + ",";
			currPrintStats.put(i + "", lineForHash);
		}

		//wait for all BGClients to start and complete their warmup
		for (int i = 0; i < stThreads.size(); i++) {
			try {
				stThreads.get(i).join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		lineForHash = "{ \"Type\":\"Overall\", \"Threads\":" + currThreadCount
				+ ", \"WorkloadPercentage\":1.00}";
		currPrintStats.put("overall", lineForHash);
		System.out.println("\n\n\n#######Attempt for " + currThreadCount
				+ " threads\n\n");

		System.out
		.println("All listeners and their BG client processes have started and created connections. Need to send startSimulation msg.");
		// send a start msg to the BGClients so they will start the simulation
		for (int i = 0; i < numBGClients; i++) {
			BGRaters.get(i).start();
		}

		// wait for the BGstarter threads to end and gather their results
		int totalActionCount = 0;
		double totalThroughput = 0;
		double totalActThroughput = 0;
		double totalStaleness = 0;
		double totalConfidence = 0;
		String logLine = "#" + currThreadCount + "#";
		String UIGraphLine = "{\"Results\": [ ";

		for (int i = 0; i < numBGClients; i++) {
			try {
				BGRaters.get(i).join();
				DateFormat dateFormat = new SimpleDateFormat(
						"yyyy/MM/dd HH:mm:ss");
				Date date = new Date();
				String time = dateFormat.format(date);
				totalActionCount += BGRaters.get(i).getActionCount();
				logLine += time + "," + logs[i]
						+ BGRaters.get(i).getThreadCount() + ","
						+ BGRaters.get(i).getSimDuration() + ","
						+ BGRaters.get(i).getSessionCount() + ","
						+ BGRaters.get(i).getThroughput() + ","
						+ BGRaters.get(i).getActionCount() + ","
						+ BGRaters.get(i).getActThroughput() + ","
						+ BGRaters.get(i).getRampedSimDuration() + ","
						+ BGRaters.get(i).getRampedSessionCount() + ","
						+ BGRaters.get(i).getRampedThroughput() + ","
						+ BGRaters.get(i).getRampedActionCount() + ","
						+ BGRaters.get(i).getRampedActThroughput() + ", "
						+ BGRaters.get(i).getStalenessPerc() + ","
						+ BGRaters.get(i).getConfidence() + ","
						+ BGRaters.get(i).succeeded() + ",#,";
				UIGraphLine += "{\"Type\":\"" + clientInfo.get(i).get("ip")
						+ ":" + clientInfo.get(i).get("port")
						+ "\", \"Threads\":" + BGRaters.get(i).getThreadCount()
						+ ", \"Throughput\":"
						+ BGRaters.get(i).getActThroughput()
						+ ", \"Confidence\":" + BGRaters.get(i).getConfidence()
						+ ", \"Staleness\":"
						+ BGRaters.get(i).getStalenessPerc() + "},";
				totalThroughput += BGRaters.get(i).getRampedThroughput();
				totalActThroughput += BGRaters.get(i).getRampedActThroughput();
				totalStaleness += BGRaters.get(i).getStalenessPerc()
						* (1.0/numBGClients);
				totalConfidence += BGRaters.get(i).getConfidence()
						* (1.0/numBGClients);
				if (maxMonitorRound < BGRaters.get(i).getMonitoringRound()
						&& finalRound)
					maxMonitorRound = BGRaters.get(i).getMonitoringRound();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		logLine = "#" + totalActThroughput + "#" + logLine;
		logger.write(logLine);
		UIGraphLine += "{\"Type\":\"Overall\", \"Threads\":" + currThreadCount
				+ ", \"Throughput\":" + totalActThroughput
				+ ", \"Confidence\":" + totalConfidence + ", \"Staleness\":"
				+ totalStaleness + "}";
		UIGraphLine += "]}";
		UIGraph.write(UIGraphLine);

		// validate the info got from each of the clients and do rating
		int totalSuccess = 0;
		for (int i = 0; i < numBGClients; i++) {
			if (BGRaters.get(i).succeeded())
				totalSuccess++;
		}

		// create the result object
		rr = new RatingResult(currThreadCount, totalActionCount,
				totalThroughput, totalActThroughput, totalConfidence,
				totalStaleness, totalSuccess, maxMonitorRound,
				finalRound, Integer.parseInt(commonProps.get("ratingunit")));
		return rr;
	}
}
