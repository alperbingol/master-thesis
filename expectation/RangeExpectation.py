from expectation.Expectation import Expectation

class RangeExpectation(Expectation):
    def __init__(self, column, dimension, add_info={}):
        super().__init__(column, dimension, add_info)

    def test(self, ge_df):
        min_value = self.add_info.get("min_value")
        max_value = self.add_info.get("max_value")
        ge_df.expect_column_values_to_be_between(
            column=self.column, 
            min_value=min_value, 
            max_value=max_value,
            meta={"dimension": self.dimension}
        )
