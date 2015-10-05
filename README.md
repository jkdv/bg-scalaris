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
    "resources": []
  }
* Resources (key = a value of "rid" with a prefix "r") <br>
  {
    "rid": "",
    "creatorid": "",
    "walluserid": "",
    "type": "",
    "body": "",
    "doc": "",
    "manipulation": [
      {
        "mid": "",
        "modifierid": "",
        "type": "",
        "content": "",
        "timestamp": ""
      },
      ...
    ]
  }
* Manipulation <br>
  TBD
