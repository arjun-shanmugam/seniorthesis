import pandas as pd

test_df = pd.DataFrame([[1,2,3], [4, 5, 6], [7, 8, 9]])

test_series = pd.Series([1, 2, 3])

print(test_df)
print(test_series)
print(test_df.multiply(test_series, axis=0))