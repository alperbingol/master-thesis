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

dq_df = create_df_from_dq_results(dq_results)
print(dq_df.to_string())