import pandas as pd
from sklearn.preprocessing import StandardScaler
import numpy as np
from constants import Variables
from typing import List, Union


def get_value_variable_names(df, analysis: str):
    all_columns = pd.Series(df.columns)  # Create Series containing all columns.
    value_columns = all_columns[all_columns.str.contains(analysis)]  # Create Series containing only value var. names.
    value_columns = pd.Series(value_columns.sort_values().reset_index(drop=True))  # Sort and reset index.
    non_value_variable_strings = [f"pre_treatment_change_in_{analysis}", f"total_twenty_seventeen_{analysis}",
                                  f"relative_pre_treatment_change_in_{analysis}", f"total_twenty_nineteen_{analysis}"]
    value_columns = value_columns.loc[~value_columns.isin(non_value_variable_strings)]
    print(value_columns)
    int_to_period_dictionary = value_columns.str.replace(f"_{analysis}", "", regex=False).to_dict()
    period_to_int_dictionary = {v: k for k, v in list(int_to_period_dictionary.items())}
    return value_columns.tolist(), period_to_int_dictionary, int_to_period_dictionary

def prepare_df_for_DiD(df: pd.DataFrame, analysis: str, treatment_date_variable: str,
                       pre_treatment_covariates: List[str],
                       missing_indicators: List[str],
                       value_vars: List[str],
                       period_to_int_dictionary):
    if analysis not in Variables.outcomes:
        raise ValueError("Unrecognized argument for parameter analysis.")

    # Add latest docket date dummies
    latest_docket_month_dummies = pd.get_dummies(df['latest_docket_month'],
                                                 drop_first=True)
    df = pd.concat([df, latest_docket_month_dummies], axis=1)
    [pre_treatment_covariates.append(col) for col in latest_docket_month_dummies.columns]
    
    # Reshape from wide to long.
    df = pd.melt(df,
                 id_vars=['case_number', treatment_date_variable, 'judgment_for_plaintiff'] + pre_treatment_covariates,
                 value_vars=value_vars, var_name='month', value_name=analysis)
    df = df.sort_values(by=['case_number', 'month'])

    # Standardize control variables.
    [pre_treatment_covariates.remove(missing_indicator) for missing_indicator in missing_indicators]
    df.loc[:, pre_treatment_covariates] = StandardScaler().fit_transform(df[pre_treatment_covariates])
    [pre_treatment_covariates.append(missing_indicator) for missing_indicator in missing_indicators]

    # Convert months from string format to integer format.
    df.loc[:, 'month'] = df['month'].str.replace(f"_{analysis}", '', regex=False).replace(period_to_int_dictionary)
    df.loc[:, treatment_date_variable] = df[treatment_date_variable].replace(period_to_int_dictionary)

    # Set treatment month to np.NaN for untreated observations.
    never_treated_mask = (df['judgment_for_plaintiff'] == 0)
    df.loc[never_treated_mask, treatment_date_variable] = np.NaN

    # Generate numeric version of case_number.
    df.loc[:, 'case_number_numeric'] = df['case_number'].astype('category').cat.codes.astype(int)

    # Set index.
    df = df.set_index(['case_number_numeric', 'month'])
    return df
