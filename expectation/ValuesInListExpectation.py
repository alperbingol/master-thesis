from expectation.Expectation import Expectation

class ValuesInListExpectation(Expectation):
    def __init__(self, column, dimension, add_info = {}):
        super().__init__(column, dimension, add_info)

    def test(self, ge_df):
        if "value_set" not in self.add_info:
            raise ValueError("Missing 'value_set' in add_info for ValuesInListExpectation")
        value_set = self.add_info["value_set"]
        #print(f"Adding ValuesInListExpectation for column: {self.column} with value_set: {value_set} and dimension: {self.dimension}")
        result = ge_df.expect_column_values_to_be_in_set(column=self.column, value_set=value_set, meta = {"dimension": self.dimension})
        
        #print(f"Result of adding ValuesInListExpectation: {result}")
        #print(f"Current expectations in the dataset after adding ValuesInListExpectation: {ge_df.get_expectation_suite().expectations}")
        
    def print(self):
        print(f"ValuesInListExpectation for column: {self.column} with value_set: {self.add_info['value_set']} and dimension: {self.dimension}")
    