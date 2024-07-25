import pandas as pd
from data_quality.DataQuality import DataQuality
from utils.utils import create_df_from_dq_results
import json


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