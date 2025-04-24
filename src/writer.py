import os
import pandas as pd
from dateutil.parser import parse

local_data_directory = os.getenv("LOCAL_DATA_DIRECTORY", "data")

def write_event(json_record, offset):    
    df = pd.DataFrame(json_record, index=[0])
    
    event_time = df['event_time'].values[:1][0]
    date_obj = parse(event_time[0:19])
    date_time = date_obj.strftime("%y-%m-%d_%H-%M-%S")    
    event_type = df['event_type'].values[:1][0]
    filename = date_time + '_offset_' + offset + '_' + event_type

    df.to_json(f"./{local_data_directory}/{filename}.json", orient='records')

    return filename

