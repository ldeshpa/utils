""" Importing libraries"""
from __future__ import absolute_import

from pprint import pprint
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

import argparse
import logging
import re

parser = argparse.ArgumentParser(description="Load CSV from GS to CloudSQL")
parser.add_argument('project', help='Google Cloud Project Name')
parser.add_argument('instance', help='Google CloudSQL Instance Name')
parser.add_argument('File_Uri', help='Google Cloud Storage File URI')
parser.add_argument('CloudSQL_DB', help='Google CloudSQL database name')
parser.add_argument('TableName', help='Google CloudSQL Table name')
parser.add_argument('COLUMN_NAMES', help='Google CloudSQL Column names')

args = parser.parse_args()

instances_import_request_body = {"importContext":{"kind":"sql#importContext","fileType":"csv","uri":args.File_Uri,"database":args.CloudSQL_DB,"csvImportOptions":{"table":args.TableName,"columns":[args.COLUMN_NAMES]}}}

#pprint(args.project)
#pprint(args.instance)
#pprint(instances_import_request_body)

logging.info('AUTHENTICATION IN PROGRESS...')
credentials = GoogleCredentials.get_application_default()
service = discovery.build('sqladmin', 'v1beta4',credentials=credentials)
#pprint([method for method in dir(service.instances()) if callable(getattr(service.instances(), method))])
request = service.instances().import_(project=args.project,instance=args.instance,body=instances_import_request_body)
response = request.execute()
#pprint([method for method in dir(response) if callable(getattr(response, method))])
operationID=(response.get('name'))
pprint(operationID)


