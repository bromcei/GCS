import uuid


def get_project_name(json_obj):
    try:
        return json_obj.get('unique_id').split('.')[1]
    except Exception as e:
        return None

def get_schema_name(json_obj):
    try:
        return json_obj.get('unique_id').split('.')[-2]
    except Exception as e:
        return None

def get_table_name(json_obj):
    try:
        return json_obj.get('unique_id').split('.')[-1]
    except Exception as e:
        return None

def get_time_since_last_row_arrived_in_s(json_obj):
    try:
        return float(json_obj.get('max_loaded_at_time_ago_in_s'))
    except Exception as e:
        return None

def get_filter_field(json_obj):
    try:
        return json_obj.get('criteria').get('filter').split(',')[1].strip()
    except Exception as e:
        return None

def get_filter_type(json_obj):
    try:
        return json_obj.get('criteria').get('filter').split(' ')[-2]
    except Exception as e:
        return None

def get_filter_value(json_obj):
    try:
        return int(json_obj.get('criteria').get('filter').split(' ')[-1])
    except Exception as e:
        return None

def get_two_nested_val(json_obj, key_1, key_2, data_type):
    try:
        res_value = json_obj.get(key_1).get(key_2)
        if data_type == 'str':
            return res_value
        if data_type == 'int':
            return int(res_value)
        if data_type == 'float':
            return float(res_value)
        return None
    except Exception as e:
        return None

def get_started_at(json_obj):
    try:
        return [timing.get('started_at') for timing in json_obj.get('timing') if timing.get('name') == 'compile'][0]
    except Exception as e:
        return None

def get_completed_at(json_obj):
    try:
        return [timing.get('completed_at') for timing in json_obj.get('timing') if timing.get('name') == 'execute'][0]
    except Exception as e:
        return None

def get_three_nested_val(json_obj, key_1, key_2, key_3, data_type):
    try:
        res_value = json_obj.get(key_1).get(key_2).get(key_3)
        if data_type == 'str':
            return res_value
        if data_type == 'int':
            return int(res_value)
        if data_type == 'float':
            return float(res_value)
        return None
    except Exception as e:
        return None

def parse_json_record(json_record, price_tb=5):
    return {
            "id": f"{str(uuid.uuid4())}",
            'project_name': get_project_name(json_record),
            "schema_name": get_schema_name(json_record),
            "table_name": get_schema_name(json_record),
            "latest_loaded_at": json_record.get('max_loaded_at'),
            "queried_at": None,
            "time_since_last_row_arrived_in_s": get_time_since_last_row_arrived_in_s(json_record),
            "status": json_record.get('status'),
            "filter_field": get_filter_field(json_record),
            "filter_type": get_filter_type(json_record),
            "filter_value": get_filter_value(json_record),
            "warn_after_period": get_three_nested_val(json_record, 'criteria', 'warn_after', 'period', 'str'),
            "warn_after_value": get_three_nested_val(json_record, 'criteria', 'warn_after', 'count', 'int'),
            "error_after_period": get_three_nested_val(json_record, 'criteria', 'error_after', 'period', 'str'),
            "error_after_value": get_three_nested_val(json_record, 'criteria', 'error_after', 'count', 'int'),
            "bytes_processed": get_two_nested_val(json_record, 'adapter_response', 'bytes_processed', 'int'),
            "bytes_billed": get_two_nested_val(json_record, 'adapter_response', 'bytes_billed', 'int'),
            "job_location": get_two_nested_val(json_record, 'adapter_response', 'location', 'str'),
            "job_project_id": get_two_nested_val(json_record, 'adapter_response', 'project_id', 'str'),
            "slot_ms": get_two_nested_val(json_record, 'adapter_response', 'slot_ms', 'int'),
            "price": get_two_nested_val(json_record, 'adapter_response', 'bytes_billed', 'float') / 1000000000000 * price_tb,
            "started_at": get_started_at(json_record),
            "completed_at": get_completed_at(json_record)
    }

