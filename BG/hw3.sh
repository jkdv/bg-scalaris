#!/usr/bin/env bash

rm ./DistrStats
rm ./FinalResultsTestDS.TestDSClient.txt
rm ./Results.txt
rm ./read*.txt
rm ./update*.txt

/home/heetae/repos/scalaris/bin/scalarisctl -n firstnode@127.0.0.1 stop
#java -cp ".:./lib/*:./build/*:./bin/*" BGListener ./bin/listenerConfig.txt &> ./logs/listener.log &
sleep 5s

configs=("coordinatorConfig_noimg_thrd10_vl.txt" "coordinatorConfig_noimg_thrd100_vl.txt" "coordinatorConfig_noimg_thrd1000_vl.txt" "coordinatorConfig_noimg_thrd10_l.txt" "coordinatorConfig_noimg_thrd100_l.txt" "coordinatorConfig_noimg_thrd1000_l.txt" "coordinatorConfig_noimg_thrd10_h.txt" "coordinatorConfig_noimg_thrd100_h.txt" "coordinatorConfig_noimg_thrd1000_h.txt" "coordinatorConfig_img_thrd10_vl.txt" "coordinatorConfig_img_thrd100_vl.txt" "coordinatorConfig_img_thrd1000_vl.txt" "coordinatorConfig_img_thrd10_l.txt" "coordinatorConfig_img_thrd100_l.txt" "coordinatorConfig_img_thrd1000_l.txt" "coordinatorConfig_img_thrd10_h.txt" "coordinatorConfig_img_thrd100_h.txt" "coordinatorConfig_img_thrd1000_h.txt")

for i in "${configs[@]}"
do
	/home/heetae/repos/scalaris/bin/scalarisctl -m -n firstnode@127.0.0.1 -p 14195 -y 8000 -t first -d start
	sleep 5s
	java -cp ".:./lib/*:./build/*:./bin/*" Coordinator ./bin/$i &> ./logs/$i.log
	sleep 5s
	/home/heetae/repos/scalaris/bin/scalarisctl -n firstnode@127.0.0.1 stop
	sleep 5s
done

