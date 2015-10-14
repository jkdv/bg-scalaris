# BG Benchmark for Scalaris
Implementation of BG client for Scalaris
http://www.bgbenchmark.org/BG/manual.html

## Schema
* Users (key = a value of "userid" with a prefix "u") <br>
  {
    "userid": "",
    "username": "",
    "pw": "",
    "fname": "",
    "lname": "",
    "gender": "",
    "dob": "",
    "jdate": "",
    "ldate": "",
    "address": "",
    "email": "",
    "tel": "",
    "tpic": "",
    "pic": "",
    "pendingFriends": [],
    "confirmedFriends": [],
    "resources": [],
    "createdResources": []
  }
* Resources (key = a value of "rid" with a prefix "r") <br>
  {
    "rid": "",
    "creatorid": "",
    "walluserid": "",
    "type": "",
    "body": "",
    "doc": "",
    "manipulation": {
      "<Manipulation ID>": {
        "mid": "<Manipulation ID>",
        "modifierid": "",
        "type": "",
        "content": "",
        "timestamp": ""
      },
      ...
    }
  }

## Preparation

### Compile
* git clone https://github.com/jkdv/bg-scalaris.git
* cd bg-scalaris/BG
* ant dbcompile-testds
* ant

### Edit Scalaris property file
* vi ./scalaris.properties

### Checking if everything is ready
* java -cp ".:./lib/\*:./build/\*" edu.usc.bg.BGMainClass onetime -schema -db TestDS.TestDSClient

### Populating data
* java -cp ".:./lib/\*:./build/\*" edu.usc.bg.BGMainClass onetime -load -db TestDS.TestDSClient -P ./workloads/populateDB -p insertimage=true -p imagesize=12 -p threadcount=8 -p usercount=1000

### Testing CLI for each action
* java -cp ".:./lib/\*:./build/\*" edu.usc.bg.FunctionCommandLine -db TestDS.TestDSClient

## Running benchmark using BGClient
* java -cp ".:./lib/\*:./build/\*" edu.usc.bg.BGMainClass onetime -t -db TestDS.TestDSClient -P ./workloads/RealisticActions -p maxexecutiontime=30 -p usercount=1000 -p initapproach=querydata -p insertimage=true

## Running Benchmark using BGCoordinator and BGListener
* java -cp ".:./lib/\*:./build/\*:./bin/\*" BGListener ./bin/listenerConfig.txt
* java -cp ".:./lib/\*:./build/\*:./bin/\*" Coordinator ./bin/coordinatorConfig.txt
