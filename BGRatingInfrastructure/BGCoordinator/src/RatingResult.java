/**                                                                                                                                                                                
 * Copyright (c) 2014 USC Database Laboratory All rights reserved. 
 *
 * Authors:  Sumita Barahmand and Shahram Ghandeharizadeh                                                                                                                            
 *        http://bgbenchmark.org/BG/coordinator.html                                                                                                                                                                         
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


/**
 * keeps a track of the output for evert benchmarking rounds
 * 
 * @author Sumita Barahmand
 * 
 */

class RatingResult {
	/*
	 *  Number of actions attempted to be issued against the data store per unit of time
	 */
	double _totalThroughput; 
	/*
	 *  Number of actions actually issued against the data store per unit of time
	 */
	double _totalActThroughput;
	/*
	 *  Total percentage of unpredictable data produced by the data store
	 */
	double _totalStaleness;
	/*
	 *  Number of clients satisfying the SLA requirement
	 */
	double _numSucceeded;
	/*
	 *  Percentage of actions satisfying the SLA average response time
	 */
	double _totalConfidence;
	/*
	 *  Total number of actions actually issued against the data store 
	 */
	int _totalActionCount;
	int _maxMonitoringRound;
	/*
	 *  If set to true the coordinator runs its final round of experiment using the duration specified in the SLA
	 */
	boolean _finalRound = false;
	/*
	 *  Identifies the duration of the experiment conducted
	 */
	int _executionDuration;


	public int getDuration() {
		return _executionDuration;
	}

	public boolean get_finalRound() {
		return _finalRound;
	}

	public int get_maxMonitoringRound() {
		return _maxMonitoringRound;
	}

	public double get_totalThroughput() {
		return _totalThroughput;
	}

	public double get_totalActThroughput() {
		return _totalActThroughput;
	}

	public double get_totalStaleness() {
		return _totalStaleness;
	}

	public double get_numSucceeded() {
		return _numSucceeded;
	}


	public double get_totalConfidence() {
		return _totalConfidence;
	}

	public int get_totalActionCount() {
		return _totalActionCount;
	}

	public void set_numSucceeded(int val) {
		_numSucceeded = val;
	}


	/*
	 * COnstructs the rating result object for each round of experiment
	 */
	public RatingResult(int currThreadCount, int totalActionCount,
			double totalThroughput, double totalActThroughput,
			double totalConfidence, double totalStaleness, double totalSuccess,
			int maxMonitoringRound, boolean finalRound,
			int duration) {
		_numSucceeded = totalSuccess;
		_totalThroughput = totalThroughput;
		_totalActThroughput = totalActThroughput;
		_totalActionCount = totalActionCount;
		_totalConfidence = totalConfidence;
		_totalStaleness = totalStaleness;
		_maxMonitoringRound = maxMonitoringRound;
		_finalRound = finalRound;
		_executionDuration = duration;
	}
}

