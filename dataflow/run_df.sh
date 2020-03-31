export INPUT='data/test_input.csv'
export OUTPUT='data/test_output.csv'
export TEMP=''
export STAG=''
export PROJECT=''
export MAIN='word_count'


#python3  -m  $MAIN     --project $PROJECT   --zone us-central1-a     --max_num_workers 1    --runner DirectRunner    --input_file $INPUT --output_file $OUTPUT --temp_location     $TEMP   --staging_location $STAG    --requirements_file  ./requirements.txt --setup_file ./setup.py

python3  -m  $MAIN        --zone us-central1-a     --max_num_workers 1    --runner DirectRunner    --input_file $INPUT --output_file $OUTPUT     --requirements_file  ./requirements.txt --setup_file ./setup.py
