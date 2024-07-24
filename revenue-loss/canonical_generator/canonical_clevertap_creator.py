import os
import re
import ruamel.yaml
import datetime

def default_fields_list():
    list_of_default_fields = [
        {"name": "yyyymmdd", "type_of_field": "function", "value": "yyyyMMdd_ist_from_utc_epoch(epoch)"},
        {"name": "hhmmss", "type_of_field": "function", "value": "hhmmss_ist_from_utc_epoch(epoch)"},
        {"name": "quarter_hour", "type_of_field": "function", "value": "quarter_hour_ist_from_utc_epoch(epoch)"},
        {"name": "hh", "type_of_field": "function", "value": "hour_ist_from_utc_epoch(epoch)"},
        {"name": "epoch", "type_of_field": "Cast", "value": "long"},
        {"name": "updated_epoch", "type_of_field": "Cast", "value": "long", "source_field": "epoch"},
        {"name": "updated_hhmmss", "type_of_field": "function", "value": "hhmmss_ist_from_utc_epoch(epoch)"},
        {"name": "updated_quarter_hour", "type_of_field": "function","value": "quarter_hour_ist_from_utc_epoch(epoch)"},
        {"name": "updated_yyyymmdd", "type_of_field": "function", "value": "yyyyMMdd_ist_from_utc_epoch(epoch)"},
        {"name": "updated_hh", "type_of_field": "function", "value": "hour_ist_from_utc_epoch(epoch)"},
    ]
    return list_of_default_fields

def convert_camel_to_snake_case(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    name = re.sub('__([A-Z])', r'_\1', name)
    name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name)
    return  name.lower()

def json_extract_cleaner(source_field, json_path_field):
    source_field = source_field.split('(')[1].strip() # split on (, pick source json field and strip unwanted chars
    json_path_field = json_path_field.split(')')[0].strip() # split on ) bracket and pick json path
    return [source_field, json_path_field]

def get_json_fields_in_list(json_fields_list : list):
    list_of_json_fields = [x.strip() for x in json_fields_list.split(',')]
    final_list = []
    for i in range(0,len(list_of_json_fields),2):
        final_list.append(json_extract_cleaner(list_of_json_fields[i],list_of_json_fields[i+1]))
    return final_list

def create_mapping():
    mappings = {}
    json_fields = get_json_extact_fields()
    json_fields_list = get_json_fields_in_list(json_fields)
    for fields in json_fields_list:
        source_field = fields[0]
        path_string = fields[1].replace("'","")
        if len(path_string.split(".")) > 3:
            name = convert_camel_to_snake_case('_'.join(path_string.split('.')[-3:]))
        else:
            name = convert_camel_to_snake_case('_'.join(path_string.split('.')[-2:]))
        if "CTLatitude" in path_string:
            name = name.replace("ct_","CT")
        if "CTLongitude" in path_string:
            name = name.replace("ct_","CT")
        if "$_" in name:
            name = name.replace("$_","")
        mappings[name] = {}
        mappings[name]['source_field'] = source_field
        mappings[name]['type'] = 'String'
        mappings[name]['json_path'] = path_string
        mappings[name]['using'] = "JsonExtract"
    ist_list = ["created_ist", "created_utc"]
    for itr in ist_list:
        mappings[itr] = {}
        mappings[itr]['json_path'] = " "
        mappings[itr]['source_field'] = " "
        mappings[itr]['type'] = "String"
        mappings[itr]['using'] = "Expression"
        mappings[itr]['expression'] = "date_time_from_utc_epoch(epoch,\"GMT+5:30\")"
        if "utc" in itr:
            mappings[itr]['expression'] = "date_time_from_utc_epoch(epoch,\"GMT\")"
    mappings['event_props_ct_location'] = {
        'json_lat_path': '$.eventProps.CTLatitude',
        'json_long_path': '$.eventProps.CTLongitude',
        'source_field': 'eventProps',
        'using': 'Location',
        'type': 'Double'
    }
    mappings['event_props_location'] = {
        'json_lat_path': '$.eventProps.latitude',
        'json_long_path': '$.eventProps.longitude',
        'source_field': 'eventProps',
        'using': 'Location',
        'type': 'Double'
    }
    for fields in default_fields_list():
        name = fields["name"]
        type_of_field = fields["type_of_field"]
        value = fields["value"]
        mappings[name] = {}
        if type_of_field == "function":
            mappings[name]['type'] = "string"
            mappings[name]['source_field'] = " "
            mappings[name]['using'] = "Expression"
            mappings[name]['expression'] = value
        else:
            mappings[name]['using'] = "Cast"
            mappings[name]['type'] = value
            mappings[name]['source_field'] = name
            if name == "updated_epoch":
                mappings[name]['source_field'] = "epoch"

    final_data_dump = {
        "partition_columns": ['yyyymmdd', 'hh'],
        "mappings": mappings,
        "sql_join_mappings": {
            "event_props_ct_location_hex_8": {
                "columns_to_select": {
                    "city": "string",
                    "cluster": "string"
                },
                "hdfs_path": "gs://production-data-datasets/storage/city_cluster_hex_logs_v2",
                "format": "parquet",
                "filter": {
                    "type": "LATEST",
                    "value": "executiondate"
                },
                "join_columns": {
                    "event_props_ct_location_hex_8": "hex_id"
                },
                "join_type": "left",
                "join_optimization": "BROADCAST"
            },
            "event_props_location_hex_8": {
                "columns_to_select": {
                    "city": "string",
                    "cluster": "string"
                },
                "hdfs_path": "gs://production-data-datasets/storage/city_cluster_hex_logs_v2",
                "format": "parquet",
                "filter": {
                    "type": "LATEST",
                    "value": "executiondate"
                },
                "join_columns": {
                    "event_props_location_hex_8": "hex_id"
                },
                "join_type": "left",
                "join_optimization": "BROADCAST"
            }
        }

    }
    output_yaml_file = "output/clevertap/" + event_type + "/" + event_name + "/" + event_name + ".yaml"
    ruamel.yaml.round_trip_dump(final_data_dump, open(output_yaml_file, "w"),
                                default_flow_style=False)
    print("Canonical mapping created")

def create_job_configs(event_name, event_type):
    mapping_file_name = event_type + "/v1.0.0/iceberg/" + event_name + ".yaml"
    event_name = event_name
    event_type = event_type
    print("Creating Canonical Job Configs")
    yaml_dict = yaml_config_dict()
    dag_configs = yaml_dict['dag_configs'][0]
    dag_configs['name'] = "CANONICAL_" + event_type.upper() + "_" + event_name.upper()
    dag_configs['run_config']['schedule_config']['start_date'] = (datetime.datetime.utcnow() - datetime.timedelta(days=7)).strftime("%Y-%m-%dT") + str("00:00:00")
    dag_configs['run_config']['cluster_config']['name'] = dag_configs['run_config']['cluster_config']['name']  + event_name.lower().replace("_","-")
    job_configs = dag_configs['job_configs'][0]
    job_configs['input_config']['base_data_path'] = "clevertap-" + event_type + "/clevertap_" + event_type + "_events_v2_delta/eventname=" + event_name
    job_configs['input_config']['mapping_filepath'] = job_configs['input_config']['mapping_filepath'] + mapping_file_name
    job_configs['output_config']['table_name'] = "iceberg_" + event_type + "_" + event_name + "_v1"
    job_configs['output_config']['output_path'] = "storage/clevertap-" + event_type + "/" + event_name
    os.makedirs("output/clevertap/" + event_type + "/" + event_name, exist_ok=True)
    output_yaml_file = "output/clevertap/" + event_type + "/" + event_name + "/job_configs.yaml"
    ruamel.yaml.round_trip_dump(yaml_dict, open(output_yaml_file, "w"),
                                default_flow_style=False)
    print("Canonical Jobs config created")

def yaml_config_dict():
    yaml_dict = ruamel.yaml.round_trip_load(open("resources/clevertap_config_template.yaml", "rb"),
                                            preserve_quotes=True)
    return yaml_dict

def remove_raw_view_prefix_query():
    raw_view = clevertap_raw_view()
    raw_view_removed_select_columns = "JSON" + raw_view.split("JSON", 1)[1]
    return raw_view_removed_select_columns

def get_json_extact_fields():
    raw_view_removed_select_columns = remove_raw_view_prefix_query()
    json_field_string = raw_view_removed_select_columns.rsplit("FROM")[0]
    return json_field_string


def event_name_type():
    raw_view_removed_select_columns = remove_raw_view_prefix_query()
    event_name = raw_view_removed_select_columns.rsplit("FROM")[1].split("eventname = ")[1].strip(')').strip("'")
    if "captain" in raw_view_removed_select_columns.split("FROM")[1]:
        event_type = "captain"
    else:
        event_type = "customer"
    return event_name, event_type

def clevertap_raw_view():
    raw_view = """CREATE VIEW hive.raw.clevertap_customer_safety_alert SECURITY DEFINER AS SELECT eventname , yyyymmdd , hhmmss , quarter_hour , epoch , created_ist , JSON_EXTRACT_SCALAR(deviceInfo, '$.deviceInfo.appVersion') deviceInfo_appVersion , JSON_EXTRACT_SCALAR(deviceInfo, '$.deviceInfo.browser') deviceInfo_browser , JSON_EXTRACT_SCALAR(deviceInfo, '$.deviceInfo.dimensions.height') deviceInfo_dimensions_height , JSON_EXTRACT_SCALAR(deviceInfo, '$.deviceInfo.dimensions.unit') deviceInfo_dimensions_unit , JSON_EXTRACT_SCALAR(deviceInfo, '$.deviceInfo.dimensions.width') deviceInfo_dimensions_width , JSON_EXTRACT_SCALAR(deviceInfo, '$.deviceInfo.dpi') deviceInfo_dpi , JSON_EXTRACT_SCALAR(deviceInfo, '$.deviceInfo.make') deviceInfo_make , JSON_EXTRACT_SCALAR(deviceInfo, '$.deviceInfo.model') deviceInfo_model , JSON_EXTRACT_SCALAR(deviceInfo, '$.deviceInfo.osVersion') deviceInfo_osVersion , JSON_EXTRACT_SCALAR(deviceInfo, '$.deviceInfo.sdkVersion') deviceInfo_sdkVersion , JSON_EXTRACT(profile, '$.profile.all_identities') profile_all_identities , JSON_EXTRACT_SCALAR(profile, '$.profile.email') profile_email , JSON_EXTRACT_SCALAR(profile, '$.profile.identity') profile_identity , JSON_EXTRACT_SCALAR(profile, '$.profile.name') profile_name , JSON_EXTRACT_SCALAR(profile, '$.profile.phone') profile_phone , JSON_EXTRACT_SCALAR(profile, '$.profile.platform') profile_platform , JSON_EXTRACT_SCALAR(eventProps, '$.eventProps.action') eventProps_action , JSON_EXTRACT_SCALAR(eventProps, '$.eventProps.area') eventProps_area , JSON_EXTRACT_SCALAR(eventProps, '$.eventProps.cleverTapId') eventProps_cleverTapId , JSON_EXTRACT_SCALAR(eventProps, '$.eventProps.currentCity') eventProps_currentCity , JSON_EXTRACT_SCALAR(eventProps, '$.eventProps.currentCluster') eventProps_currentCluster , JSON_EXTRACT_SCALAR(eventProps, '$.eventProps.epoch') eventProps_epoch , JSON_EXTRACT_SCALAR(eventProps, '$.eventProps.from') eventProps_from , JSON_EXTRACT_SCALAR(eventProps, '$.eventProps.latitude') eventProps_latitude , JSON_EXTRACT_SCALAR(eventProps, '$.eventProps.location_accuracy') eventProps_location_accuracy , JSON_EXTRACT_SCALAR(eventProps, '$.eventProps.longitude') eventProps_longitude , JSON_EXTRACT_SCALAR(eventProps, '$.eventProps.occurred_date') eventProps_occurred_date , JSON_EXTRACT_SCALAR(eventProps, '$.eventProps.source') eventProps_source , JSON_EXTRACT_SCALAR(eventProps, '$.eventProps.userId') eventProps_userId , JSON_EXTRACT_SCALAR(eventProps, '$.eventProps.CTAppVersion') eventProps_CTAppVersion , JSON_EXTRACT_SCALAR(eventProps, '$.eventProps.CTLatitude') eventProps_CTLatitude , JSON_EXTRACT_SCALAR(eventProps, '$.eventProps.CTLongitude') eventProps_CTLongitude , JSON_EXTRACT_SCALAR(eventProps, '$.eventProps.CTSource') eventProps_CTSource , JSON_EXTRACT_SCALAR(eventProps, '$.eventProps.CTSessionId') eventProps_CTSessionId , JSON_EXTRACT(deviceInfo, '$.deviceInfo') deviceInfo_data , JSON_EXTRACT(profile, '$.profile') profile_data , JSON_EXTRACT(eventProps, '$.eventProps') eventProps_data FROM hive.raw.clevertap_customer_events_master WHERE (eventname = 'safety_alert')"""
    return raw_view


if __name__ == "__main__":
    view_name = ""

    event_name,event_type = event_name_type()
    create_job_configs(event_name,event_type)
    create_mapping()