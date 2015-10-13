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

## Up and Running

### Compile
* ant dbcompile-testds
* ant

### Checking if everything is ready
* java -cp ".:./lib/\*:./build/\*" edu.usc.bg.BGMainClass onetime -schema -db TestDS.TestDSClient

### Populating data
* java -cp ".:./lib/\*:./build/\*" edu.usc.bg.BGMainClass onetime -load -db TestDS.TestDSClient -P ./workloads/populateDB -p insertimage=true -p imagesize=2 -p threadcount=3 -p usercount=150

### Testing CLI for each action
* java -cp ".:./lib/\*:./build/\*" edu.usc.bg.FunctionCommandLine -db TestDS.TestDSClient

### Running benchmark
* java -cp ".:./lib/\*:./build/\*" edu.usc.bg.BGMainClass onetime -t -db TestDS.TestDSClient -P ./workloads/RealisticActions -p maxexecutiontime=30 -p usercount=150 -p initapproach=querydata -p insertimage=true
