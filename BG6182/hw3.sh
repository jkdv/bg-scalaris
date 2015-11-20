#!/usr/bin/env bash

rm ./DistrStats
#rm ./FinalResultsTestDS.TestDSClient.txt
rm ./Results.txt
rm ./read*.txt
rm ./update*.txt
rm ./logs/*

/home/heetae/Repos/scalaris/bin/scalarisctl -n firstnode@127.0.0.1 stop
sleep 3s

#java -cp ".:./lib/*:./build/*:./bin/*" BGListener ./bin/listenerConfig.txt
#sleep 3s

configs=("coordinatorConfig_h.txt" "coordinatorConfig_l.txt" "coordinatorConfig_vl.txt")
#configs=("coordinatorConfig_h.txt")

for i in "${configs[@]}"
do
	/home/heetae/Repos/scalaris/bin/scalarisctl -m -n firstnode@127.0.0.1 -p 14195 -y 8000 -t first -d start
	sleep 3s
	java -cp ".:./lib/*:./build/*:./bin/*" Coordinator ./bin/$i &> ./logs/$i.log
	sleep 3s
	/home/heetae/Repos/scalaris/bin/scalarisctl -n firstnode@127.0.0.1 stop
	sleep 3s
done

