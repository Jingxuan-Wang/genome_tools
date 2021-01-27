#!/bin/bash
for i in "20171123" "20171124" "20171125"
do
sdate=$i
##edate="20171013"
##edate=$sdate
##until [ $sdate -gt $edate ]
##do

input_date=$sdate
inputPath="hdfs:///user/ashutosh/RealTime/lbs_filtered/"$input_date
confPath="/home/jingxuan/RnD/runner/FFCountingForExtrapolation/conf/FFCounting.conf"
profilePath="hdfs:///user/ashutosh/customer_profile"
homeworkPath="hdfs:///user/anhkeen/homework_201710"
cellMappingPath="hdfs:///user/ashutosh/geo_hierarchy.snappy.parquet"
outputPath="hdfs:///user/jingxuan/ffcount_for_extrapolation/sa2/event_gabba_"$input_date

source ~/spark_submit_setting.sh

hadoop fs -rmr -skipTrash $outputPath

spark-submit $SPARK_SUBMIT \
--files $confPath \
--class "FFCountingForExtrapolation" ~/RnD/JX-Analysis-assembly-1.0.jar \
--input $inputPath \
--output $outputPath \
--profile $profilePath \
--hw $homeworkPath \
--cell $cellMappingPath \
--conf $confPath 

##sdate=$(date -d "$sdate 1 day" '+%Y%m%d')
done
