from reader.JSONFileReader import JSONFileReader
#from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
import pandas as pd
from great_expectations.dataset import PandasDataset
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from expectation.NotNullExpectation import NotNullExpectation
from expectation.UniqueExpectation import UniqueExpectation
from expectation.ValuesInListExpectation import ValuesInListExpectation
import json

class DataQuality:
    
    def __init__(self, pandas_df, config_path):
        self.pandas_df = pandas_df
        self.config_path = config_path
        self.ge_df = PandasDataset(self.pandas_df)
        self.expectation_suite_name = "default_suite"
        self.expectations_loaded = False
        self.ge_df._initialize_expectations(expectation_suite=ExpectationSuite(expectation_suite_name=self.expectation_suite_name))
        #print("Available methods and attributes in self.ge_df:")
        #print(dir(self.ge_df))
        
    
    def rule_mapping(self, dq_rule):
        return {
            'check_if_not_null': NotNullExpectation,
            'check_if_unique': UniqueExpectation,
            'check_if_values_in_list': ValuesInListExpectation
        }[dq_rule]
    
    def read_config(self):
        json_reader = JSONFileReader(self.config_path)
        return json_reader.read()
    
    def add_expectation(self, expectation_instance):
        expectation_instance.test(self.ge_df)
        #print(f"Expectation {expectation_instance.__class__.__name__} added for column: {expectation_instance.column}")
        # Save the expectation suite after adding each expectation
        self.save_expectation_suite(self.expectation_suite_name)
    
    def run_test(self):
        if not self.expectations_loaded:
            config = self.read_config()
            print("Reading config and adding expectations:")
            for column in config["columns"]:
                if column["dq_rule(s)"] is None:
                    continue
                for dq_rule in column["dq_rule(s)"]:
                    expectation_class = self.rule_mapping(dq_rule["rule_name"])
                    expectation_instance = expectation_class(column["column_name"], dq_rule["rule_dimension"], dq_rule["add_info"])
                    self.add_expectation(expectation_instance)
                    print(f"Added expectation: {expectation_instance.__class__.__name__} for column: {column['column_name']}")
                    print(f"Current expectations in the dataset: {self.ge_df.get_expectation_suite().expectations}")
                    #print(f"Current dataset: {self.ge_df}")
                    
        print("Expectation suite before validation:")
        print(json.dumps(self.ge_df.get_expectation_suite().to_json_dict(), indent=2))         
        
        dq_results = self.ge_df.validate()
        #print("Validation results:")
        dq_results_dict = dq_results.to_json_dict()
        #print(json.dumps(dq_results_dict, indent=2))
        
        
        return dq_results
    
    def save_expectation_suite(self, suite_name="default_suite"):
        """"
        expectation_suite = self.ge_df.get_expectation_suite()
        print("Expectation suite before saving:")
        print(json.dumps(expectation_suite.to_json_dict(), indent=2))
        with open(f"{suite_name}.json", "w") as f:
            json.dump(expectation_suite.to_json_dict(), f, indent=2)
        print(f"Saved expectation suite: {suite_name}")
        """
        
        # Use built-in method to save the expectation suite
        self.ge_df.save_expectation_suite(suite_name)
        print(f"Saved expectation suite: {suite_name}")

    def load_expectation_suite(self, suite_name="default_suite"):                                    
        with open(f"{suite_name}.json", "r") as f:
            suite_dict = json.load(f)
            suite = ExpectationSuite(expectation_suite_name=suite_name)
            suite.expectations = [
                ExpectationConfiguration(**expectation) for expectation in suite_dict['expectations']
            ]
            suite.meta = suite_dict['meta']
            self.ge_df._initialize_expectations(expectation_suite=suite)
            self.expectations_loaded = True
        print(f"Loaded expectation suite: {suite_name}")
            
    def are_expectations_loaded(self):
        return self.expectations_loaded