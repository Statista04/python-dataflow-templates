import subprocess
import os
from models.dataflow.dlp_api.dlp_token import DlpApi
from config.config import  Config
config = Config()
from models.dataflow.dlp_api.dlp_token_v3 import DlpApi
from models.dataflow.dlp_api.inspect2 import run_join
from models.dataflow.dlp_api.export_dicom import export_dicom_instance

"""dlp tests """

project_id = 'sp-ext-sth0001-dev'
cloud_region = 'northamerica-northeast1'



#run_join()
#dlp = DlpApi()
#dlp.runonce()



#fixed config
project_id = config.project_id

#job_name = 'gcs-mllp-hcapi-streaming-v1'
#job_name = 'hcapi-bq-dlp-streaming-v1'
job_name = 'test_splits'
temp_location = config.temp_location

#varibale config
input_topic = config.gcs_topic
input_folder = config.input_folder
output_table =config.output_table
output_topic = config.hcapi_topic
mllp_topic = config.mllp_topic
dlp_table = config.dlp_table



# Batch UC2
#bashCommand = f"python  -m  df_main     --project {project_id}   --runner DataflowRunner  --input_file gs://sp-ext-sth0001-dev-staging/test_input.csv --output_table uc4_deid_dataset.test_split  --temp_location     gs://sp-ext-sth0001-dev-staging/temp   --staging_location gs://sp-ext-sth0001-dev-staging/tmp    --requirements_file ./requirements.txt  --setup_file ./setup.py"

#streaming UC1 hcapi bq - BQ - DLP
#bashCommand = f"python  -m  df_main     --project {project_id}  --runner DataflowRunner  --zone northamerica-northeast1-a     --max_num_workers 1  --job_name {job_name}  --input_topic {mllp_topic} --output_table   {output_table}  --output_table_dlp  {dlp_table}   --temp_location     {temp_location}   --staging_location {temp_location}    --requirements_file  ./requirements.txt --setup_file ./setup.py"

#streaming UC1 gcs hcapi- MLLP
#bashCommand = f"python  -m  df_main     --project {project_id}   --zone northamerica-northeast1-a     --max_num_workers 4    --runner DataflowRunner   --job_name {job_name}    --input_topic {input_topic} --output_topic   {output_topic} --temp_location     {temp_location}   --staging_location {temp_location}    --requirements_file  ./requirements.txt --setup_file ./setup.py"

#####test parameters
input_topic = 'projects/cio-insights-etl-36503c31/topics/pdc-tar-folder-dropped'
output_topic = 'projects/cio-insights-etl-36503c31/topics/test_topic'
project_id = 'cio-insights-etl-36503c31'
temp_location = 'gs://cio-insights-etl-36503c31-airflow-logs/temp_files'
bucket = 'gs://cio-insights-etl-36503c31-drop-pdc-tar-folders'

# swan pipeline
bashCommand = "python  -m  df_main     --project {}   --zone northamerica-northeast1-a     --max_num_workers 4    --runner DataflowRunner   --job_name testjob1   --input_topic {} --output_topic   {} --temp_location     {}   --staging_location {}    --requirements_file  ./requirements.txt --setup_file ./setup.py".format(project_id, input_topic,output_topic, temp_location, temp_location)



process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
output, error = process.communicate()
print(output)


