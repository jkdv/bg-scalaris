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
    "wallResources": [],
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

