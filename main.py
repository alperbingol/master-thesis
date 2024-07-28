import pandas as pd
from data_quality.DataQuality import DataQuality
from utils.utils import create_df_from_dq_results
import json
from scripts.performance import time_run_test
import logging

#logging.basicConfig(level=logging.INFO)


def create_sample_dataframe(csv_file_path, sample_size=20):
    
    df = pd.read_csv(csv_file_path)
    sample_df = df.head(sample_size)
    #print(sample_df)
    return sample_df


if __name__ == "__main__":
    
    csv_file_path = '../Data/aemo.csv'
    sampleAemo_df = create_sample_dataframe(csv_file_path)
    
    #test_df_performance = time_create_sample_dataframe(csv_file_path)
    #print("Test create sample dataframe performance:",test_df_performance, "seconds")
    
    dq = DataQuality(sampleAemo_df, "config/configAemo.json")
    dq_results = dq.run_test()
    test_run_performance = time_run_test(dq)
    print("Test run performance:",test_run_performance, "seconds")
    
    
    #print("dq_results:", dq_results)
    dq_df = create_df_from_dq_results(dq_results)
    print(dq_df.to_string())
    
    
    dq.save_expectation_suite("my_expectation_suite")
    
    
    
    #with open("my_expectation_suite.json", "r") as f:
        #suite_dict = json.load(f)
        #print("Saved expectation suite contents:")
        #print(json.dumps(suite_dict, indent=2))


"""

# Sample Pandas DataFrame using the provided dataset
data = {
    'roll_no': [1, 2, 3, 4, 5, 6, 7, 8],
    'first_name': ['Ram', 'Shyam', 'Mohan', 'Sohan', 'Rohini', 'Raj', 'Meena', 'Rani'],
    'last_name': ['Kumar', 'Kumar', None, 'Singh', 'Kumari', 'Kumar', None, 'Kumari'],
    'subject': ['Maths', 'History', 'Science', 'Maths', 'Science', 'Maths', 'Hindi', 'Sanskrit']
}
student_df = pd.DataFrame(data)
dq = DataQuality(student_df, "config/config.json")
dq_results = dq.run_test()

print("dq:", dq)
#dq.save_expectation_suite("my_expectation_suite")

#print("dq_results:", dq_results)

dq_df = create_df_from_dq_results(dq_results)
print(dq_df.to_string())

# Verify the saved expectation suite
with open("my_expectation_suite.json", "r") as f:
    suite_dict = json.load(f)
    print("Saved expectation suite contents:")
    print(json.dumps(suite_dict, indent=2))


# Loading the expectation suite for a new DataFrame
new_data = {
    'roll_no': [9, 10],
    'first_name': ['Ravi', 'Lakshmi'],
    'last_name': ['Kumar', 'Kumari'],
    'subject': ['Maths', 'History']
}
new_student_df = pd.DataFrame(new_data)

dq_new = DataQuality(new_student_df, "config/config.json")
dq_new.load_expectation_suite("my_expectation_suite")

# Check if expectations are loaded
if dq_new.are_expectations_loaded():
    print("Expectations loaded from suite.")
else:
    print("Expectations not loaded from suite.")

dq_new_results = dq_new.run_test()
#print(dq_new_results)

dq_new_df = create_df_from_dq_results(dq_new_results)
print(dq_new_df.to_string())

"""