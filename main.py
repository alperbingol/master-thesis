import pandas as pd
from data_quality.DataQuality import DataQuality
from utils.utils import create_df_from_dq_results


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
dq.save_expectation_suite("my_expectation_suite")

dq_df = create_df_from_dq_results(dq_results)
print(dq_df.to_string())


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
dq_new_results = dq_new.run_test()
#print(dq_new_results)

dq_new_df = create_df_from_dq_results(dq_new_results)
print(dq_new_df.to_string())