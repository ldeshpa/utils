from pprint import pprint
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
credentials = GoogleCredentials.get_application_default()
service = discovery.build('sqladmin', 'v1beta4',credentials=credentials)
project = 'syw-dw'
instance = 'syw-imv-dev-db'
instances_import_request_body = {"importContext": {"kind": "sql#importContext","fileType": 'csv',"uri": 'gs://syw_l0/test/cloudsqltestgzip/employee_2.csv.gz',"database": 'test',"csvImportOptions": {"table": 'Employee',"columns": ['employee_name','employee_id','department_id','employee_salary']}}}
#pprint([method for method in dir(service.instances()) if callable(getattr(service.instances(), method))])
request = service.instances().import_(project=project,instance=instance,body=instances_import_request_body)
response = request.execute()
pprint(response)
