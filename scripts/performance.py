from time import perf_counter
import functools


def timer(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        time_start = perf_counter()
        func(*args, **kwargs)
        execution_time = perf_counter() - time_start
        print(f"Function '{func.__name__}' executed in: {execution_time:.4f} seconds")
    return wrapper


@timer
def time_run_test(dq):
    print("Running data quality test...")
    dq.run_test()
    

"""
@timer
def time_create_sample_dataframe(csv_file_path, sample_size=10):
    create_sample_dataframe(csv_file_path, sample_size)
"""
    
    