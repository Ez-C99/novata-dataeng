import os, sys
import great_expectations as ge

from great_expectations.profile.user_configurable_profiler import UserConfigurableProfiler

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src')))

import data_processing as dp
from constants import DATA_PATH

def profile_data(data_path):
    data = dp.extract(data_path)
    ge_data = ge.from_pandas(data)
    
    # Define the semantic types dictionary
    semantic_types_dict = {
        "string": ["id", "email", "location", "widget_list"],
        "numeric": ["age_group", "user_score", "revenue"],
        "datetime": ["created_at"],
    }
    
    profiler = UserConfigurableProfiler(
        profile_dataset=ge_data,
        semantic_types_dict=semantic_types_dict
    )
    
    expectation_suite = profiler.build_suite()
    
    return expectation_suite

if __name__ == "__main__":
    data_path = DATA_PATH
    expectation_suite = profile_data(data_path)
    print(expectation_suite.to_json_dict())
