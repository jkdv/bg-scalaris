/**                                                                                                                                                                                
 * Copyright (c) 2014 USC Database Laboratory All rights reserved. 
 *
 * Authors:  Sumita Barahmand and Shahram Ghandeharizadeh                                                                                                                            
 *           http://bgbenchmark.org/BG/coordinator.html                                                                                                                                                                      
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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;


public class Logger {
	String _filename;
	BufferedWriter _out;
	boolean _timestamp = true;
	public Logger(String filename, boolean append, boolean timestamp){
		// Create file 
		_timestamp = timestamp;
		FileWriter fstream;
		try {
			fstream = new FileWriter(filename,append);
			_out = new BufferedWriter(fstream);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public void write(String line){
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		String time = dateFormat.format(date);
		if(_timestamp)
			line = time + "\t"+ line;
		try {
			_out.write(line);
			_out.flush();
			_out.write(" \n");
			_out.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void close(){
		try {
			_out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
