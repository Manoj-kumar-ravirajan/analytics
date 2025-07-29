1. pip install ruamel.yaml
2. in metabase , write query - "show create view raw.{your_clevertap_view_name}"
3. copy this view definition from metabase
4. open file canonical_clevertap_creator.py
5. in this file find function clevertap_raw_view()
6. replace the value of raw_view with the definition copied from metabase
7. run the script using play button on the left ( support in pycharm ) or from terminal run python canonical_clevertap_creator.py
8. go to output folder
9. copy the job_config and mapping and create a DPIC board ticket for canonical ingestion