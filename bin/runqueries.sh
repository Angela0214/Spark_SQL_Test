# #!/bin/bash

# SPARK_HOME=$1
# OUTPUT_DIR=$2
# DRIVER_OPTIONS="--driver-memory 4g --driver-java-options -Dlog4j.configuration=file:///${OUTPUT_DIR}/log4j.properties"
# EXECUTOR_OPTIONS="--executor-memory 2g --num-executors 1 --conf spark.executor.extraJavaOptions=-Dlog4j.configuration=file:///${OUTPUT_DIR}/log4j.properties --conf spark.sql.crossJoin.enabled=true"

# cd $SPARK_HOME
# divider===============================
# divider=$divider$divider
# header="\n %-10s %11s %15s\n"
# format=" %-10s %11.2f %10s %4d\n"
# width=40

# spark_version=$($SPARK_HOME/bin/spark-submit --version 2>&1 | grep -m 1 -oE 'version [0-9]+\.[0-9]+\.[0-9]+')
# echo "Spark Version: $spark_version" > ${OUTPUT_DIR}/run_summary.txt
# printf "$header" "Query" "Time(secs)" "Rows returned" > ${OUTPUT_DIR}/run_summary.txt
# printf "%$width.${width}s\n" "$divider" >> ${OUTPUT_DIR}/run_summary.txt
# for i in `cat ${OUTPUT_DIR}/runlist.txt`;
# do
#   num=`printf "%02d\n" $i`
#   explain_file="${OUTPUT_DIR}/query${num}_explain.res"
#   bin/spark-sql ${DRIVER_OPTIONS} ${EXECUTOR_OPTIONS} ${ADDITION_SPARK_OPTIONS} -database TPCDS -f ${OUTPUT_DIR}/query${num}.sql > ${OUTPUT_DIR}/query${num}.res 2>&1
#   bin/spark-sql ${DRIVER_OPTIONS} ${EXECUTOR_OPTIONS} -database TPCDS -e "EXPLAIN EXTENDED $(cat $query_file)" > "$explain_file" 2>&1
#   lines=`cat ${OUTPUT_DIR}/query${num}.res | grep "Time taken:"`
#   echo "$lines" | while read -r line;
#   do
#     time=`echo $line | tr -s " " " " | cut -d " " -f3`
#     num_rows=`echo $line | tr -s " " " " | cut -d " " -f6`
#     printf "$format" \
#        query${num} \
#        $time \
#        "" \
#        $num_rows >> ${OUTPUT_DIR}/run_summary.txt
#   done

# done
# touch ${OUTPUT_DIR}/queryfinal.res





#!/bin/bash

SPARK_HOME=$1
OUTPUT_DIR=$2
DRIVER_OPTIONS="--driver-memory 4g --driver-java-options -Dlog4j.configuration=file:///${OUTPUT_DIR}/log4j.properties"
EXECUTOR_OPTIONS="--executor-memory 2g --num-executors 1 --conf spark.executor.extraJavaOptions=-Dlog4j.configuration=file:///${OUTPUT_DIR}/log4j.properties --conf spark.sql.crossJoin.enabled=true"

cd $SPARK_HOME

divider===============================
divider=$divider$divider
header="\n %-10s %11s %15s\n"
format=" %-10s %11.2f %10d\n"
width=40

# record spark version
spark_version=$($SPARK_HOME/bin/spark-submit --version 2>&1 | grep -m 1 -oE 'version [0-9]+\.[0-9]+\.[0-9]+')
echo "Spark Version: $spark_version" > ${OUTPUT_DIR}/run_summary.txt

# set the head
printf "$header" "Query" "Time(secs)" "Rows Returned" >> ${OUTPUT_DIR}/run_summary.txt
printf "%$width.${width}s\n" "$divider" >> ${OUTPUT_DIR}/run_summary.txt

for i in `cat ${OUTPUT_DIR}/runlist.txt`; do
  num=`printf "%02d\n" $i`
  query_file="${OUTPUT_DIR}/query${num}.sql"
  result_file="${OUTPUT_DIR}/query${num}.res"
  explain_file="${OUTPUT_DIR}/query${num}_explain.res"


  bin/spark-sql ${DRIVER_OPTIONS} ${EXECUTOR_OPTIONS} -database TPCDS -e "EXPLAIN EXTENDED $(cat $query_file)" > "$explain_file" 2>&1

  # specific optimization
    # Initialize an empty list to store optimizations
  optimizations=()

  # Check for multiple optimizations in the EXPLAIN FORMATTED output
  
  if grep -q "SortMergeJoin" "$explain_file"; then
      optimizations+=("Sort Merge Join")
  fi
  if grep -q "Index Scan" "$explain_file"; then
      optimizations+=("Index Scan")
  fi
  if grep -q "Repartitioning" "$explain_file"; then
      optimizations+=("Repartitioning Used")
  fi
  if grep -q "Dynamic Partition Pruning" "$explain_file"; then
      optimizations+=("Partition Pruning")
  fi
  if grep -q "BroadcastHashJoin" "$explain_file"; then
      optimizations+=("Broadcast Hash Join")
  fi

  # If no optimization was found, set it to "N/A"
  if [ ${#optimizations[@]} -eq 0 ]; then
      optimization="N/A"
  else
      # Join multiple optimizations with commas
      optimization=$(IFS=, ; echo "${optimizations[*]}")
  fi

  
  start_time=$(date +%s)
  bin/spark-sql ${DRIVER_OPTIONS} ${EXECUTOR_OPTIONS} -database TPCDS -f "$query_file" > "$result_file" 2>&1
  end_time=$(date +%s)
  
  duration=$((end_time - start_time))

  # Get Time taken and Rows returned
  time_taken=$(grep "Time taken:" "$result_file" | awk '{print $3}')
  num_rows=$(grep "Time taken:" "$result_file" | awk '{print $6}')

  # if num_rows did not get data, set 0
  num_rows=${num_rows:-0}
  

  if grep -q "FAILED" "$result_file"; then
      status="FAILED"
  else
      status="SUCCESS"
  fi

  # write the results to summary.txt
  printf "$format" \
       query${num} \
       $duration \
       $num_rows  >> ${OUTPUT_DIR}/run_summary.txt

done

touch ${OUTPUT_DIR}/queryfinal.res
