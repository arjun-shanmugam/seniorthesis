import pandas as pd
from matplotlib import pyplot as plt

from figure_utilities import plot_scatter_with_error_bars

y = pd.Series(1, index=range(0, 10))
y_upper = pd.Series(2, index=range(0, 10))
y_lower = pd.Series(0, index=range(0, 10))

fig, ax = plt.subplots()
plot_scatter_with_error_bars(ax,
                             y.index,
                             y,
                             y_upper,
                             y_lower)
plt.show()