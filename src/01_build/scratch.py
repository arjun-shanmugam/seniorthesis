import pandas as pd
from zillow_utilities import GET_zestimate_history
df = GET_zestimate_history(81826510, "1")
print(df)