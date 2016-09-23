#!/bin/bash
set -o pipefail
######################################################################################
# Script 	:  	MySQL_TaskRunner.sh
#			This script is called in MySQL_MainScheduler.sh
#
# Author 	:	Lohith Deshpande / Shubham Gupta
#
# Date		: 	08-Nov'2015
#
# Modification  :       06/09 - Changed the email failure notification similar to Control-M
#
#######################################################################################

usage() {
  cat <<EOF

        MySqlRunner Usage:
        -b - Batch Name (required)
        -j - JobID to be executed. Must be in JOB_<number> format. (required)
	-r - Extract Timestamp (optional)
EOF
}

while getopts "b:j:r:" OPTION
do
    case $OPTION in
        b) BATCHNAME=$OPTARG;;
        j) JOBID=$OPTARG;;
	r) RunTs=$OPTARG;;
    esac
done


if [[ -z ${BATCHNAME} ]] || [[ -z ${JOBID} ]]
then usage
     exit -1
fi

if [[ -z ${RunTs} ]]
then
RunTs=`date +"%Y-%m-%d %H:%M:%S"`
fi

#variables to store info
USER_NAME='root'
M_PASSWORD='1mvSYW'
HOST_NAME='104.155.183.221'
PORT='3306'
DB_NAME='L0_map_tbls'
CTRLTBL='mapcontroltbl'
CTRLTBL_LOG=$CTRLTBL"_log"
MAP_ONCALL='ldeshpa@searshc.com,vtarvec@searshc.com,asaxen1@searshc.com'
HOST='GoogleComputeEngine'
SCHEDULER='MAP MySQL Scheduler'

TEMP_FILE=/tmp/TMP_MAPMADE_LOG_LOCATION_$$

#function to execute mysql commands
exec_mysql(){
        mysql --port=$PORT --host=$HOST_NAME -u $USER_NAME --password=$M_PASSWORD $DB_NAME  \
        -BNse "$1" 2>/dev/null
        return $?
}

#function to send email notificatoin incase of failure
function emailnotification_fail
{
echo -e ''${SCHEDULER}' job failed, details below:

SCHEDULER : '${SCHEDULER}'
HOST : '${HOST}'
BATCHNAME : '${BATCHNAME}'
ODATE : '${RunTs}'
GROUPNAME : '${GROUPNAME}'
SUBGROUPNAME : '${SUBGROUPNAME}'
JOBID : '${JOBID}'
JOBNAME : '${JOBNAME}'
FREQUENCY : '${FREQUENCY}'
'${LOG_LOCATION}'
INCIDENT# : '$$''| sed 's/^/  /g' | mail -s "$HOST | $SCHEDULER | ERROR:Batch $BATCHNAME failed at Job# $JOBID - $JOBNAME" $MAP_ONCALL
}


#Preliminary checks to identify a restart or a fresh run
TBLEXISTCHK=`exec_mysql "SHOW CREATE TABLE $CTRLTBL;"`
JOBEXISTCHK=`exec_mysql "SELECT COUNT(*) FROM $CTRLTBL WHERE BatchNm = '$BATCHNAME' AND JobID = '$JOBID';"`
JOBRUNCHK=`exec_mysql "SELECT COUNT(*) FROM $CTRLTBL WHERE BatchNm = '$BATCHNAME' AND JobID = '$JOBID' AND JobStatus='RUN';"`
JOBACTVFLG=`exec_mysql "SELECT COUNT(*) FROM $CTRLTBL WHERE BatchNm = '$BATCHNAME' AND JobID = '$JOBID' AND JobActiveFlag='Y';"`
LOGTBLCHK=`exec_mysql "SHOW CREATE TABLE $CTRLTBL_LOG;"`

if [ -z "$TBLEXISTCHK" ] || [ "$JOBEXISTCHK" -ne 1 ]
then
echo "ERROR:Either table $CTRLTBL or jobid $JOBID doesn't exist. Exiting."
exit -1
fi

if [ "$JOBRUNCHK" -eq 1 ] || [ "$JOBACTVFLG" -ne 1 ]
then
echo "ERROR:Either jobid $JOBID is currently running or is not active. Exiting."
exit -1
fi

if [ -z "$LOGTBLCHK" ]
then
echo "ERROR: Log table $CTRLTBL_LOG doesn't exist.Exiting"
exit -1
fi

COMMAND=`exec_mysql "SELECT CommandLine FROM $CTRLTBL WHERE BatchNm = '$BATCHNAME' AND JobID = '$JOBID' AND JobActiveFlag='Y' AND JobStatus IN ('WAIT','FAIL');"`
GROUPNAME=`exec_mysql "SELECT GroupName FROM $CTRLTBL WHERE BatchNm = '$BATCHNAME' AND JobID = '$JOBID' AND JobActiveFlag='Y' AND JobStatus IN ('WAIT','FAIL');"`
SUBGROUPNAME=`exec_mysql "SELECT SubGroupName FROM $CTRLTBL WHERE BatchNm = '$BATCHNAME' AND JobID = '$JOBID' AND JobActiveFlag='Y' AND JobStatus IN ('WAIT','FAIL');"`
JOBNAME=`exec_mysql "SELECT JobName FROM $CTRLTBL WHERE BatchNm = '$BATCHNAME' AND JobID = '$JOBID' AND JobActiveFlag='Y' AND JobStatus IN ('WAIT','FAIL');"`
FREQUENCY=`exec_mysql "SELECT Frequency FROM $CTRLTBL WHERE BatchNm = '$BATCHNAME' AND JobID = '$JOBID' AND JobActiveFlag='Y' AND JobStatus IN ('WAIT','FAIL');"`
 
if [ -z "$COMMAND" ]
then
echo "ERROR : No command to execute. Exiting"
exit -1
fi

#Ensure to assign correct RunTs value if invalid	
        if [ -z "$RunTs" ];
        then
        RunTs=`date +"%Y-%m-%d %H:%M:%S"`
        fi

	#insert first record into the log table and update status on control table
	exec_mysql "INSERT INTO $CTRLTBL_LOG SELECT JobID,JobSeqNbr,BatchNm,'$RunTs',GroupName,SubGroupName,JobName
		    ,JobActiveFlag,Frequency,CommandLine,now(),NULL,'RUN' FROM $CTRLTBL WHERE BatchNm = '$BATCHNAME' AND JobID = '$JOBID';" 
	exec_mysql "UPDATE $CTRLTBL SET JobStatus='RUN' WHERE BatchNm = '$BATCHNAME' AND JobID = '$JOBID' AND JobStatus IN ('WAIT','FAIL');" 
	#execute the command line here
    eval $COMMAND | tee -a $TEMP_FILE
	ret_code=$?
	
	if [ "$ret_code" -eq 0 ];
	then	
	#update the status and timestamp on log and control table once the command execution is succesful
	exec_mysql "UPDATE $CTRLTBL_LOG SET JobEndTs=now(),JobStatus='DONE' WHERE BatchNm = '$BATCHNAME' AND JobID = '$JOBID' AND JobStatus='RUN';" 
	exec_mysql "UPDATE $CTRLTBL SET JobStatus='DONE' WHERE BatchNm = '$BATCHNAME' AND JobID = '$JOBID' AND JobStatus='RUN';" 
	else
        #update the status and timestamp on log and control table if the command execution fails
	exec_mysql "UPDATE $CTRLTBL_LOG SET JobEndTs=now(),JobStatus='FAIL' WHERE BatchNm = '$BATCHNAME' AND JobID = '$JOBID' AND JobStatus='RUN';" 
    exec_mysql "UPDATE $CTRLTBL SET JobStatus='FAIL' WHERE BatchNm = '$BATCHNAME' AND JobID = '$JOBID' AND JobStatus='RUN';"

	LOG_LOCATION=`cat $TEMP_FILE | grep LOG_LOCATION`
	emailnotification_fail
	
	fi 

rm $TEMP_FILE
exit $ret_code
