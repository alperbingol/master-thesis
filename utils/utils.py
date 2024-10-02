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
            "score": float(100 - result["result"]["unexpected_percent"])
        })
        #print("result:", result)
    dq_df = pd.DataFrame(dq_data)
    return dq_df


def create_summary_report(dq_results):
    summary = {
        "total_checks": 0,
        "total_passed": 0,
        "total_failed": 0,
        "failed_details": []
    }
    for result in dq_results["results"]:
        summary["total_checks"] += 1
        if result["success"]:
            summary["total_passed"] += 1
        else:
            summary["total_failed"] += 1
            summary["failed_details"].append({
                "column": result["expectation_config"]["kwargs"]["column"],
                "expectation_type": result["expectation_config"]["expectation_type"],
                "unexpected_count": result["result"].get("unexpected_count", 0),
                "unexpected_percent": result["result"].get("unexpected_percent", 0)
            })
    
    return summary

