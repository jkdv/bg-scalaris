# BG Benchmark for Scalaris
Implementation of BG client for Scalaris
http://www.bgbenchmark.org/BG/manual.html

## Schema
* Users (key = a value of "userid") <br>
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
* Resources (key = a value of "rid") <br>
  {
  "rid": "",
  "creatorid": "",
  "walluserid": "",
  "type": "",
  "body": "",
  "doc": ""
  }
* Manipulation <br>
  TBD
