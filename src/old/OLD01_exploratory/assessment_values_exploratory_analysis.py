"""
assessment_values_exploratory_analysis.py

Performs exploratory analysis on the assessment values data.
"""
import pandas as pd
import matplotlib.pyplot as plt

from src.old.utilities.figure_utilities import plot_histogram
import numpy as np
import os

ASSESSOR_FILE = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/data/02_intermediate/assessor_data.csv"
OUTPUT_FIGURES = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/output/01_exploratory/figures"
OUTPUT_TABLES = "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/output/01_exploratory/tables"

df = pd.read_csv(ASSESSOR_FILE)

# Plot histogram of log-total property values.
fig, ax = plt.subplots(1, 1)
plot_histogram(ax=ax,
               x=np.log(df['TOTAL_VAL'][df['TOTAL_VAL'] >= 0] + 1),
               title="Histogram of Log(Total Values) of Massachusetts Properties",
               xlabel="Log of Total Value ($)")
plt.savefig(os.path.join(OUTPUT_FIGURES, "hist_TOTAL_VAL.png"), bbox_inches='tight')
plt.close(fig)

