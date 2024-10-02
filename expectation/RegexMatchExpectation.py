from expectation.Expectation import Expectation

class RegexMatchExpectation(Expectation):
    def __init__(self, column, dimension, add_info={}):
        super().__init__(column, dimension, add_info)

    def test(self, ge_df):
        regex_pattern = self.add_info.get("regex_pattern")
        ge_df.expect_column_values_to_match_regex(
            column=self.column, 
            regex=regex_pattern,
            meta={"dimension": self.dimension}
        )
