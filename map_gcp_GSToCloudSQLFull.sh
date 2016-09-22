#===================================================================================================
# Sears Holdings Corporation, 2016
#
# Name: map_gcp_BQSelectInsertFull.sh
# Date: 29 Jan 2015
# Author: Vikas Tarvecha (Vikas.Tarvecha@searshc.com)
# Purpose: Create and Insert data in Table on BigQuery using Select Query.
#
# Usage: ./map_gcp_GSToCloudSQLFull.sh -p [Project-Name] -s [SQL-Instance-Name] -i [input-directory] -d [Cloud-SQL-Database-Name] -t [Cloud-SQL-Table-Name] -l [list-of-columns]
#
# History of revison:
# Date		Author		Comments
# 08/22/2016	Vikas Tarvecha	Initial Version of Create/Insert Table on BigQuery using Select Query
#====================================================================================================
#!/bin/sh
Usage(){
#clear
echo "
This script loads data from Google Storage to Cloud-SQL. 
Usage: ./map_gcp_GSToCloudSQLFull.sh -p [Project-Name] -s [SQL-Instance-Name] -i [input-directory] -d [Cloud-SQL-Database-Name] -t [Cloud-SQL-Table-Name] -l [list-of-columns]
        OPTIONS :
	-p : Google Cloud Project Name.
	-s : Google CloudSQL instance Name.
	-i : Google Storage Input Directory Name
	-d : CloudSQL Database Name
	-t : CloudSQL Table Name
	-l : List of Columns. [This can be automated in future releases]
"
exit 9
  }

while getopts p:s:i:d:t:l: opt
do
        case $opt in
		p) PROJECT_NAME=$OPTARG;;
		s) CLOUDSQL_INSTANCE_NAME=$OPTARG;;
		i) GS_INPUT_DIRECTORY=$OPTARG;;
		d) CLOUDSQL_DATABASE=$OPTARG;;
		t) CLOUDSQL_TARGET_TABLE=$OPTARG;;
		l) COLUMN_LIST=$OPTARG;;
        esac
done

FILE_ARRAY=""
FORMATED_COLUMN_LIST=""
LOG_FILE='/appl/map/logs/map_gcp_GSToCloudSQLFull_log_'`date +%Y%m%d%H%M%S`'.log'

function INFO
{
	LOG_INPUT=$1
	LOG_TIMESTAMP=`date "+%Y-%m-%d %H:%M:%S"`
	echo  "INFO[${LOG_TIMESTAMP}]: $LOG_INPUT"
	echo  "INFO[${LOG_TIMESTAMP}]: $LOG_INPUT" >> $LOG_FILE
}

function WARN
{
	WARN_INPUT=$1
	WARN_TIMESTAMP=`date "+%Y-%m-%d %H:%M:%S"`
	echo "WARN[${WARN_TIMESTAMP}: $WARN_INPUT]"
	echo "WARN[${WARN_TIMESTAMP}: $WARN_INPUT]" >> $LOG_FILE
}

function ERROR
{
	ERROR_INPUT=$1
	ERROR_TIMESTAMP=`date "+%Y-%m-%d %H:%M:%S"`
	echo "ERROR[$ERROR_TIMESTAMP]: $ERROR_INPUT"
	echo "ERROR[$ERROR_TIMESTAMP]: $ERROR_INPUT" >> $LOG_FILE
}

function CheckCommandline
{
#Check if Project Name is passed.
INFO "Checking Command-line Options"
if [[  -z "$PROJECT_NAME" ]]; then
	ERROR "Project Name is missing."
	Usage
	exit 1
else
   #export property config
   INFO "PRJECT NAME : "$PROJECT_NAME
fi

#Check if CloudSQL Instance Name is passed
if [[  -z "$CLOUDSQL_INSTANCE_NAME" ]]; then
	ERROR "CloudSQL Instance is missing."
	Usage
	exit 1
else
   #export property config
   INFO "CloudSQL Instance Name : "$CLOUDSQL_INSTANCE_NAME
fi

#Check if Google Storage Input directory is passed
if [[  -z "$GS_INPUT_DIRECTORY" ]]; then
	ERROR "Google Storage Input directory is missing."
	Usage
	exit 1
else
   #export property config
   INFO "Google Storage Input directory : "$GS_INPUT_DIRECTORY
fi

#Check if Google CloudSQL Target Table Name is passed
if [[  -z "$CLOUDSQL_TARGET_TABLE" ]]; then
	ERROR "Google CloudSQL Target Table Name  is missing."
	Usage
	exit 1
else
   #export property config
   INFO "Google CloudSQL Target Table Name  : "$CLOUDSQL_TARGET_TABLE
fi

#Check if Google CloudSQL Column List is passed
if [[  -z "$COLUMN_LIST" ]]; then
	ERROR "Google CloudSQL Column List  is missing."
	Usage
	exit 1
else
   #export property config
   INFO "Google CloudSQL Column List  : "$COLUMN_LIST
fi
}

#Generate file list to call it multiple times. 
function GetFileListAndLoadData
{
FILE_STRING=`gsutil ls -p $PROJECT_NAME $GS_INPUT_DIRECTORY`
FILE_ARRAY=(${FILE_STRING// / })
for EachFile in "${FILE_ARRAY[@]:1}"
do
  INFO "Loading File : ${EachFile}"
  LOAD_START_TIME=`date +%s`
  OperationID=`python map_gcp_GSToCloudSQL.py $PROJECT_NAME $CLOUDSQL_INSTANCE_NAME $EachFile $CLOUDSQL_DATABASE $CLOUDSQL_TARGET_TABLE $COLUMN_LIST`
  OperationID=`echo $OperationID | cut -d\' -f2` 
  INFO "Operation ID For this Job : "$OperationID
  # FOLLOWING COMMAND IS IN IT'S ALPHA PHASE. CAN CHANGE WITHOUT NOTICE. PLEASE CHECK gcloud DOCUMENTATION IF DOESN'T WORK AS EXPECTED.
  gcloud sql operations wait $OperationID -i $CLOUDSQL_INSTANCE_NAME
  LOAD_END_TIME=`date +%s`
  INFO "Operation with ID : $OperationID Finished"
  TIME_DIFF=$(($LOAD_END_TIME - $LOAD_START_TIME))
  INFO "TIME TAKE FOR THIS FILE TO LOAD : "$TIME_DIFF" Seconds."
done
INFO "All files loaded sucessfully."
}

CheckCommandline
GetFileListAndLoadData
