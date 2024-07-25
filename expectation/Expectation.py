from abc import ABC, abstractmethod

class Expectation(ABC):
    def __init__(self, column, dimension, add_info = {}):
        self.column = column #name of the column on which the expectation rule will be applied
        self.dimension = dimension #Completeness, Uniqueness, Validity, Accuracy, and Consistency
        self.add_info = add_info #optional parameter for adding any additional information required for the test
    
    @abstractmethod
    def test(self, ge_df):
        pass

        
        