import pandas as pd
from data_quality.DataQuality import DataQuality
from utils.utils import create_df_from_dq_results, create_summary_report
import json
from scripts.performance import time_run_test

"""

# Create logs directory if not exists
if not os.path.exists('logs'):
    os.makedirs('logs')
    
# Logging configuration
logger = logging.getLogger('DataQualityLogger')
logger.setLevel(logging.INFO)

# Create a file handler
log_file = 'logs/data_quality_service.log'
file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.INFO)

# Create a console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create a logging format
formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s', "%Y-%m-%dT%H:%M:%S")
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Log initial messages
logger.critical("~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ DataQuality service is starting ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~")
logger.warning("Setting the logging level to INFO")
logger.warning(f"Logs will be written to the file {log_file}")


"""

def create_sample_dataframe(csv_file_path, sample_size=10):
    
    df = pd.read_csv(csv_file_path)
    sample_df = df.head(sample_size)
    #print(sample_df)
    return sample_df

def print_summary_report(summary):
    print("\nData Quality Summary Report")
    print("=" * 30)
    print(f"Total Checks: {summary['total_checks']}")
    print(f"Total Passed: {summary['total_passed']}")
    print(f"Total Failed: {summary['total_failed']}")
    print("\nFailed Details:")
    for detail in summary["failed_details"]:
        print(f"  Column: {detail['column']}")
        print(f"  Expectation Type: {detail['expectation_type']}")
        print(f"  Unexpected Count: {detail['unexpected_count']}")
        print(f"  Unexpected Percent: {detail['unexpected_percent']:.2f}%")
        print("-" * 30)


if __name__ == "__main__":
    
    csv_file_path = '../Data/aemo.csv'
    sampleAemo_df = create_sample_dataframe(csv_file_path)
    
    #test_df_performance = time_create_sample_dataframe(csv_file_path)
    #print("Test create sample dataframe performance:",test_df_performance, "seconds")
    
    dq = DataQuality(sampleAemo_df, "config/configAemo.json", "expectation_suite_aemo")
    dq_results = dq.run_test()
    
    #time_run_test(dq)
    #print("Test run performance: ", test_run_performance, "seconds")
    
    #print("dq_results:", dq_results)
    dq_df = create_df_from_dq_results(dq_results)
    print(dq_df.to_string())
    
    
    dq.save_suit()
    
    """
    with open("my_expectation_suite", "r") as f:
        suite_dict = json.load(f)
        print("Saved expectation suite contents:")
        print(json.dumps(suite_dict, indent=2))
    """
    
    data = {
    'roll_no': [1, 2, 3, 4, 5, 6, 7, 8],
    'first_name': ['Ram', 'Shyam', 'Mohan', 'Sohan', 'Rohini', 'Raj', 'Meena', 'Rani'],
    'last_name': ['Kumar', 'Kumar', None, 'Singh', 'Kumari', 'Kumar', None, 'Kumari'],
    'subject': ['Maths', 'History', 'Science', 'Maths', 'Science', 'Maths', 'Hindi', 'Sanskrit'],
    'marks': [85, 70, 65, 60, 75, 80, 70, 105]
    }   
    
    student_df = pd.DataFrame(data)
    dqTest = DataQuality(student_df, "config/configTest.json", "expectation_suite_test")
    dq_results_test = dqTest.run_test()
    
    dqTest.save_suit()
    
    
    dq_df_test = create_df_from_dq_results(dq_results_test)
    print(dq_df_test.to_string())
    
    # Create and print summary report
    summary_report = create_summary_report(dq_results_test)
    print_summary_report(summary_report)
    
    print("-" * 30)
    print("Loading the saved expectation suite for a new DataFrame...")
    
    dqTest.load_expectation_suite("expectation_suite_test_old")
    
    # Check if expectations are loaded
    if dqTest.are_expectations_loaded():
        print("Expectations loaded from suite.")
    else:
        print("Expectations not loaded from suite.")

    dq_new_results = dqTest.run_test()

    dq_new_df = create_df_from_dq_results(dq_new_results)
    print(dq_new_df.to_string())



"""
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