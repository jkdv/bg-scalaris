#!/usr/bin/env bash

configs=("coordinatorConfig_noimg_thrd10_vl.txt" "coordinatorConfig_noimg_thrd100_vl.txt" "coordinatorConfig_noimg_thrd1000_vl.txt" "coordinatorConfig_noimg_thrd10_l.txt" "coordinatorConfig_noimg_thrd100_l.txt" "coordinatorConfig_noimg_thrd1000_l.txt" "coordinatorConfig_noimg_thrd10_h.txt" "coordinatorConfig_noimg_thrd100_h.txt" "coordinatorConfig_noimg_thrd1000_h.txt" "coordinatorConfig_img_thrd10_vl.txt" "coordinatorConfig_img_thrd100_vl.txt" "coordinatorConfig_img_thrd1000_vl.txt" "coordinatorConfig_img_thrd10_l.txt" "coordinatorConfig_img_thrd100_l.txt" "coordinatorConfig_img_thrd1000_l.txt" "coordinatorConfig_img_thrd10_h.txt" "coordinatorConfig_img_thrd100_h.txt" "coordinatorConfig_img_thrd1000_h.txt")

for i in "${configs[@]}"
do
	cp coordinatorConfig.txt $i
done

