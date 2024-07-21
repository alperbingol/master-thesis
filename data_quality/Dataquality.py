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
        self.ge_df._initialize_expectations(expectation_suite=ExpectationSuite(expectation_suite_name=self.expectation_suite_name))
        
        
    def rule_mapping(self, dq_rule):
        return {
            'check_if_not_null': NotNullExpectation,
            'check_if_unique': UniqueExpectation,
            'check_if_values_in_list': ValuesInListExpectation
        }[dq_rule]
        
    def _get_expectation(self):
        class_obj = globals()[self.rule_mapping()]
        return class_obj(self.extractor_args)
    
    def read_config(self):
        json_reader = JSONFileReader(self.config_path)
        return json_reader.read()
    
    def add_expectation(self, expectation_instance):
        expectation_instance.test(self.ge_df)
    
    def run_test(self):
        config = self.read_config()
        for column in config["columns"]:
            if column["dq_rule(s)"] is None:
                continue
            for dq_rule in column["dq_rule(s)"]:
                expectation_obj = self.rule_mapping(dq_rule["rule_name"])
                expectation_instance = expectation_obj(column["column_name"], dq_rule["rule_dimension"], dq_rule["add_info"])
                self.add_expectation(expectation_instance)
    
        dq_results = self.ge_df.validate()
        return dq_results
    
    def save_expectation_suite(self, suite_name="default_suite"):
        expectation_suite = self.ge_df.get_expectation_suite()
        with open(f"{suite_name}.json", "w") as f:
            json.dump(expectation_suite.to_json_dict(), f, indent=2)

    def load_expectation_suite(self, suite_name="default_suite"):                                    
        with open(f"{suite_name}.json", "r") as f:
            suite_dict = json.load(f)
            suite = ExpectationSuite(expectation_suite_name=suite_name)
            suite.expectations = [
                ExpectationConfiguration(**expectation) for expectation in suite_dict['expectations']
            ]
            suite.meta = suite_dict['meta']
            self.ge_df._initialize_expectations(expectation_suite=suite)