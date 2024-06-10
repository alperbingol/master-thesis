import pandas as pd

def create_df_from_dq_results(dq_results):
    dq_data = []
    for result in dq_results["results"]:
        if result["success"] == True:
            status = 'PASSED'
        else:
            status = 'FAILED'
        dq_data.append({
            "column": result["expectation_config"]["kwargs"]["column"],
            "dimension": result["expectation_config"]["meta"]["dimension"],
            "status": status,
            "expectation_type": result["expectation_config"]["expectation_type"],
            "unexpected_count": result["result"]["unexpected_count"],
            "element_count": result["result"]["element_count"],
            "unexpected_percent": result["result"]["unexpected_percent"],
            "percent": float(100 - result["result"]["unexpected_percent"])
        })
    dq_df = pd.DataFrame(dq_data)
    return dq_df