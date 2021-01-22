# dhus_opensearch_data_gaps
A tool for comparing content in various DHuS Opensearch endpoints

The work process should be as follows:
1) Harvest content from the DHuS Opensearch endpoints specified in your
   "input_params.yaml" using "collect_Sentinel_titles_mp.py"

2) Investigate the difference between two endpoints using the
   "sort_and_differentiate_files.py"
