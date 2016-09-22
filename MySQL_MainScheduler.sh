#!/bin/bash
######################################################################################
# Script 	:  	MySQL_MainScheduler.sh
#
# Purpose 	: 	To execute the command lines in sequence from MongoDB control table
#			and restart from failed location.
#			This python script prepares a temporary log table which is dropped
#			at the end of process.
#
# Author 	:	Lohith Deshpande / Shubham Gupta
#
# Date		: 	08-Nov'2015
#
#######################################################################################

usage() {
  cat <<EOF

        MySqlRunner Usage:
        -b - Batch name (required)
EOF
}

while getopts "b:" OPTION
do
    case $OPTION in
        b) BATCHNAME=$OPTARG;;
    esac
done

if [ ! -f /appl/map/ctrlfile/core/map_db_user_cred ]
then
echo "ERROR: File /appl/map/ctrlfile/core/map_db_user_prod_cred doesn't exist. Exiting"
exit -1
fi

# Load the config file
. /appl/map/ctrlfile/core/map_db_user_cred

if [ $? -ne 0 ]
then
echo "ERROR : Something wrong with the file permissions on /appl/map/ctrlfile/core/map_db_user_prod_cred . Exiting"
exit -1
fi

#variables to store info
# Parameters/Variables
SCRIPT_PATH=`dirname $0`
SCRIPT_NAME=`basename $0`
USER_NAME=$MYSQL_USER
M_PASSWORD=$MYSQL_PWD
HOST_NAME=$MYSQL_HOSTNAME
PORT=$MYSQL_PORT
DB_NAME=$MYSQL_SCHEMA
CTRLTBL=$MYSQL_CONTROL_TBL
CTRLTBL_LOG=$CTRLTBL"_LOG"
EMAILADDR=$EMAIL_ADDR_DIST
MAP_ONCALL=$MAPONCALL
COMMANDLIST=/tmp/TMP_MAPMADE_JobList_$$
TMPFINALSCRIPT=/tmp/TMP_MAPMADE_FinalScript_$$.sh
TIMESTAMP=`date +%Y%m%d%H%M%S`

# Defalut log folder
LOGDIR=$MYSQL_LOG_DIR
LOGFILE="$LOGDIR/${SCRIPT_NAME}_${BATCHNAME}_$TIMESTAMP.log"

if [[ -z ${BATCHNAME} ]]
then usage
     exit -1
fi

# Check the taskrunner

if [ ! -f $SCRIPT_PATH/MySQL_TaskRunner.sh ]
then
echo "ERROR : File $SCRIPT_PATH/MySQL_TaskRunner.sh doesn't exist. Existing"
exit -1
fi 

#function to execute mysql commands
exec_mysql(){
        mysql --port=$PORT --host=$HOST_NAME -u $USER_NAME --password=$M_PASSWORD $DB_NAME  \
        -BNse "$1" 2>/dev/null
        return $?
}

#Preliminary checks to identify a restart or a fresh run
TBLEXISTCHK=`exec_mysql "SHOW CREATE TABLE $CTRLTBL;"`
LOGTBLCHK=`exec_mysql "SHOW CREATE TABLE $CTRLTBL_LOG;"`
BATCHNM_CHK=`exec_mysql "SELECT COUNT(*) FROM $CTRLTBL WHERE BatchNm = '$BATCHNAME';"`
ActvBATCHNM_CHK=`exec_mysql "SELECT COUNT(*) FROM $CTRLTBL WHERE BatchNm = '$BATCHNAME' AND JobActiveFlag = 'Y';"`
BATCH_STATUS=`exec_mysql "SELECT JobStatus FROM $CTRLTBL WHERE BatchNm = '$BATCHNAME' AND JobActiveFlag = 'Y';" | sort -u`
AUDIT_CHK=`exec_mysql "SELECT JobID,BatchNm,count(*) FROM $CTRLTBL WHERE JobActiveFlag = 'Y' GROUP BY 1,2 HAVING COUNT(*) > 1;"`

RUNCHK=`echo "$BATCH_STATUS" | grep  'RUN'`
DONECHK=`echo "$BATCH_STATUS" | grep  'DONE'`
FAILCHK=`echo "$BATCH_STATUS" | grep  'FAIL'`
WAITCHK=`echo "$BATCH_STATUS" | grep  'WAIT'`
OTHERCHK=`echo "$BATCH_STATUS" | grep -v 'RUN\|DONE\|WAIT\|FAIL'`

if  [[ "$RUNCHK" == "RUN" ]];
then
BATCH_RUNNING='Y'
else
BATCH_RUNNING='N'
fi

if [[ -z $RUNCHK && "$DONECHK" == "DONE" && -z $FAILCHK && -z $WAITCHK ]];
then
FRESH_RUN='Y'
else
FRESH_RUN='N'
fi

if [[ -z $RUNCHK && "$FAILCHK" == "FAIL" ]];
then
FAIL_RESTART='Y'
else
FAIL_RESTART='N'
fi

if [[ -z $RUNCHK && -z $FAILCHK && "$WAITCHK" == "WAIT" ]]
then
WAIT_RESTART='Y'
else
WAIT_RESTART='N'
fi

if [ -z "$TBLEXISTCHK" ] || [ -z "$LOGTBLCHK" ]
then
echo "ERROR:Either table $CTRLTBL or its log table doesn't exist. Exiting." 
echo "ERROR:Either table $CTRLTBL or its log table doesn't exist. Exiting." >> $LOGFILE 2>&1
exit -1
fi

if [ "$AUDIT_CHK" ]
then
echo "ERROR: Audit check failed. More than 1 job found, with the JobID & BatchNm combination from control table $CTRLTBL. Exiting."
echo "ERROR: Audit check failed. More than 1 job found, with the JobID & BatchNm combination from control table $CTRLTBL. Exiting." >> $LOGFILE 2>&1
exit -1
fi

if [ "$BATCHNM_CHK" -eq 0 ] || [ "$ActvBATCHNM_CHK" -eq 0 ]
then
echo "ERROR: Either the Batch name $BATCHNAME or active jobs in it doesn't exist in control table $CTRLTBL.Exiting"
echo "ERROR: Either the Batch name $BATCHNAME or active jobs in it doesn't exist in control table $CTRLTBL.Exiting" >> $LOGFILE 2>&1
exit -1
fi

if [ "$BATCH_RUNNING" == "Y" ];
then
echo "ERROR: Looks like the batch is running.Exiting.Bye"
echo "ERROR: Looks like the batch is running.Exiting.Bye" >> $LOGFILE 2>&1
exit -1
fi

#Restart of the jobs which are in FAIL & WAIT Startus
if  [ "$FAIL_RESTART" == "Y" ];
then
RunTs=`exec_mysql "SELECT MAX(ExtractTs) FROM $CTRLTBL_LOG WHERE BatchNm = '$BATCHNAME' AND JobActiveFlag = 'Y' AND JobStatus='FAIL';"` 
	#Ensure to assign correct RunTs value if invalid
        if [ -z "$RunTs" ];
        then
        RunTs=`date +"%Y-%m-%d %H:%M:%S"`
        fi
echo "INFO: According to the data, this is a restart from previous run.."
echo "INFO: According to the data, this is a restart from previous run.." >> $LOGFILE 2>&1
exec_mysql "SELECT CONCAT_WS('|',TRIM(JobID),TRIM(JobSeqNbr)) FROM $CTRLTBL WHERE BatchNm = '$BATCHNAME' AND JobStatus IN ('FAIL','WAIT') AND JobActiveFlag = 'Y' ORDER BY JobID;" > $COMMANDLIST 
fi

#Fresh run code
if [ "$FRESH_RUN" == "Y" ];
then
RunTs=`date +"%Y-%m-%d %H:%M:%S"`
echo "INFO: According to the data, this is a fresh run of batch.."
echo "INFO: According to the data, this is a fresh run of batch.." >> $LOGFILE 2>&1
exec_mysql "SELECT CONCAT_WS('|',TRIM(JobID),TRIM(JobSeqNbr)) FROM $CTRLTBL WHERE BatchNm = '$BATCHNAME' AND JobActiveFlag = 'Y' ORDER BY JobID;" > $COMMANDLIST
#update the status of all the jobs to wait condition intially
exec_mysql "UPDATE $CTRLTBL SET JobStatus='WAIT' WHERE BatchNm = '$BATCHNAME' AND JobActiveFlag = 'Y';" 
fi

#Restart of the jobs which are in WAIT status
if [ "$WAIT_RESTART" == "Y" ];
then
RunTs=`exec_mysql "SELECT MAX(ExtractTs) FROM $CTRLTBL_LOG WHERE BatchNm = '$BATCHNAME' AND JobActiveFlag = 'Y';"`
	#Ensure to assign correct RunTs value if invalid
        if [ -z "$RunTs" ];
        then
        RunTs=`date +"%Y-%m-%d %H:%M:%S"`
        fi
echo "INFO: According to the data, this is a restart from previous run.."
echo "INFO: According to the data, this is a restart from previous run.." >> $LOGFILE 2>&1
exec_mysql "SELECT CONCAT_WS('|',TRIM(JobID),TRIM(JobSeqNbr)) FROM $CTRLTBL WHERE BatchNm = '$BATCHNAME' AND JobActiveFlag = 'Y' AND JobStatus='WAIT' ORDER BY JobID;" > $COMMANDLIST
fi

if [ -z $COMMANDLIST ];
then
echo "ERROR:No Jobs to execute.Exiting"
echo "ERROR:No Jobs to execute.Exiting" >> $LOGFILE 2>&1
exit -1
fi

SEQNBR=`cat $COMMANDLIST | cut -d"|" -f2`
UNIQSEQNBR=$(echo $SEQNBR |tr " " "\n"|sort -b -V|uniq|tr "\n" " ")

echo "set -e
set -o pipefail " >> $TMPFINALSCRIPT 

for seqnbr in `echo $UNIQSEQNBR`
do
ALLJOBNBR=`cat $COMMANDLIST | grep -w ${seqnbr} | cut -d"|" -f1`
        for jobnbr in `echo $ALLJOBNBR`
        do
        echo "bash $SCRIPT_PATH/MySQL_TaskRunner.sh -b $BATCHNAME -j $jobnbr -r '$RunTs' &
        PID_$jobnbr=$"!"" >> $TMPFINALSCRIPT 
        done

        for jobnbr in `echo $ALLJOBNBR`
        do
        echo "wait $"PID_$jobnbr"" >> $TMPFINALSCRIPT
        done
done

echo "Parallel processes created succesfully, all systems GO !!"
echo "Parallel processes created succesfully, all systems GO !!" >> $LOGFILE 2>&1

echo "---------------------------------------------------------"
echo "---------------------------------------------------------" >> $LOGFILE 2>&1
echo "Starting the batch $BATCHNAME execution..."
echo "---------------------------------------------------------" >> $LOGFILE 2>&1
bash $TMPFINALSCRIPT >> $LOGFILE 2>&1
