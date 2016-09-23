############################################
# Author : Manohar Bharti(MBHART0)
# Date   : 6th/June/2016
# Description: Enable to set the dependecies for a job, Dependent job can be a whole batch or specific job withing a batch.

############################################

#SETTING IF ANY COMMAND FAILS IN SCRIPT RETURN TERMINATE THE WHOLE SCRIPT
set -e 
set -o pipefail

current_time=$(date "+%Y%m%d%H%M%S")
date_time=$(date +"%Y-%m-%d %H:%M:%S")
start_ts=$(date +"%s")

file_batch_name=""

if [ ! -z "$2" ];then
	file_batch_name=$2
fi

log_file="/logs/map/mysqlscheduler/BatchJobWthcr_${file_batch_name}_MySQLJobWatcher_${current_time}.log"

echo "You can see the log in file $log_file"
echo "$date_time BatchJobWatcher script is started ...." >> $log_file

#iParsing arguments and conveting it to arrays
#batch_args="testbtch2[JOB_3],testbtch[JOB_1 JOB_2]"
if [[ "$#" -ne 2 || -z "$1" || -z "$2" ]]; then
  printf  "Please check the argument!\nUsage: bash MySQL_BatchJobWatcher.sh \"bathcid1[job1 job2],batchid2[job3],batchid3[*]\" \"BatchName\"\n"
  echo "Please check the argument! Usage: bash MySQL_BatchJobWatcher.sh \"bathcid1[job1 job2],batchid2[job3],batchid3[*]\" \"BatchName\"" >> $log_file
  exit -1
fi

#exporting database connection string
source /appl/map/ctrlfile/core/map_db_user_cred
batch_args="$1"
IFS=',' read -r -a batch_arr <<< "${batch_args}"

#printf '%s\n' "${batch_arr[@]}"
function exec_mysql {
local mysql_command="$1"
local mysql_result=$(mysql -N --host="$MYSQL_HOSTNAME" --port="$MYSQL_PORT" --user="$MYSQL_USER" --password="$MYSQL_PWD" -e "$mysql_command" 2>/dev/null)
echo $mysql_result
}
#Funtion to check the status of a given job
function checkIfDone {
	local btch_nm=$1
	local job_id=$2
	local hostnm=`hostname`
	local status_code=""
	local mysql_command="select JobStatus from ${MYSQL_SCHEMA}.${MYSQL_CONTROL_TBL} where BatchNm='$btch_nm' and JobID='$job_id' and JobActiveFlag='Y'"
	status_code=$(exec_mysql "$mysql_command")
echo $status_code
}

# To get the details like PRED Batch, GroupName, SubGroupName, JobName
function getBatchDetails {
	local job_id="$1"
	local batch_name="$2"
	local mysql_command="select concat(JobName, ', BatchNm : ',BatchNm,', Group name : ',GroupName,', Subgroupname: ', SubGroupName) from ${MYSQL_SCHEMA}.${MYSQL_CONTROL_TBL} where jobId='$job_id' and BatchNm='$batch_name' and JobActiveFlag='Y'"
	local details=$(exec_mysql "$mysql_command")
echo "$details"
}

done_code="DONE"
all_job_done="false"
counter_iteration=1
while [ $all_job_done == "false" ]
do

echo "$(date +"%Y-%m-%d %H:%M:%S") ###################### Iteration $counter_iteration #######################" >> $log_file

all_done=0
for i in "${batch_arr[@]}"
do
	batch_id="$i"
	batch_nm=`echo $batch_id | cut -d "[" -f1`
	job_ids=`echo $batch_id | cut -d "[" -f2 | cut -d "]" -f1`
	if [ "$job_ids" == "*" ];then
		mysql_command="SET SESSION group_concat_max_len = 2048;select GROUP_CONCAT(JobID SEPARATOR ' ') from ${MYSQL_SCHEMA}.${MYSQL_CONTROL_TBL} where BatchNm='$batch_nm' and JobActiveFlag='Y'  group by BatchNm"
        	job_ids=$(exec_mysql "$mysql_command")
	fi
	job_arr=($job_ids)
        
	for job_id in "${job_arr[@]}"
	do
		
         #echo "$job_id"
		is_done=$(checkIfDone $batch_nm $job_id)
		
		if [ "$is_done" != "$done_code" ];then
			all_done=1
		   	pred_job_dtls=$(getBatchDetails "$job_id" "$batch_nm")
			#echo "pred details are $pred_job_dtls"
		   	echo "$(date +"%Y-%m-%d %H:%M:%S") : Job is waiting to complete job  $pred_job_dtls" >> $log_file
		fi
	done
done
	if [ $all_done -eq 1  ];
	then
		all_job_done="false"
		end_ts=$(date +%s)
		secs=$(($end_ts - $start_ts))
		hrs=$(($secs/3600))
		echo "$(date +"%Y-%m-%d %H:%M:%S") : Job is waiting since  $hrs Hour, $(($secs%3600/60)) Minute, $(($secs%60)) Second" >> $log_file
		if [[ "$hrs" -ge 5 ]];then
			echo "Terminating the batch $batch_name as this is wating for 5 hours to complete its predecessors." >> $log_file
                        exit -1
                fi

		sleep 5m
	else
		all_job_done="true"
		 echo "$(date +"%Y-%m-%d %H:%M:%S") : All Jobs are completed" >> $log_file
	fi
counter_iteration=$((counter_iteration+1))
done

if [ $? == 0 ];then
	echo "$(date +"%Y-%m-%d %H:%M:%S") BatchJobWatcher script is completed ...." >> $log_file
else
	echo "$(date +"%Y-%m-%d %H:%M:%S") BatchJobWatcher script is failed! ...." >> $log_file
fi
