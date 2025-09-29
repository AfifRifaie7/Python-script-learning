# Mock class for testing
class panther:
    def preprocess_filter(self, type, engine): pass
    def get_file_paths(self, typ): return ["test.lis"]

data_log = {"file": ["test.lis"], "row_count": [10]}
print(data_log)

import pandas as pd
df = pd.DataFrame({"file_name": ["file1"], "row_count": [100]})
print(df.to_sql)  # Note: Needs engine, but test concat

from concurrent.futures import ProcessPoolExecutor
def worker(n): return n * 2
with ProcessPoolExecutor(2) as executor:
    results = list(executor.map(worker, [1, 2]))
print(results)  # [2, 4]