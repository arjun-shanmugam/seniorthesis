"""
Functions useful for analysis.
"""
import pandas as pd
import numpy as np


def produce_summary_statistics(df: pd.DataFrame, treatment_date_variable: str):
    """

    :param df:
    :param treatment_date_variable:
    :return:
    """
    # Panel A: Case Initiaton
    panel_A_columns = ['for_cause', 'foreclosure', 'no_cause', 'non_payment', 'for_cause_transfer',
                       'foreclosure_transfer',
                       'non_payment_transfer', 'no_cause_transfer']
    origin_columns = ['initiating_action', 'initiating_action', 'initiating_action', 'initiating_action',
                      'initiating_action', 'initiating_action', 'initiating_action', 'initiating_action']
    target_values = ["SP Summons and Complaint - Cause", "SP Summons and Complaint - Foreclosure",
                     "SP Summons and Complaint - No Cause", "SP Summons and Complaint - Non-payment of Rent",
                     "SP Transfer - Cause", "SP Transfer - Foreclosure", "SP Transfer - Non-payment of Rent",
                     "SP Transfer- No Cause"]

    for dummy_column, origin_column, target_value in zip(panel_A_columns, origin_columns, target_values):
        df.loc[:, dummy_column] = np.where(df[origin_column] == target_value, 1, 0)

    panel_A = df[sorted(panel_A_columns)].describe().T
    panel_A = pd.concat([panel_A], keys=["Panel A: Case Initiation"])

    # Panel B: Case Resolution
    panel_B_columns = ['mediated', 'dismissed', 'defaulted', 'heard']
    origin_columns = ['disposition_found', 'disposition_found', 'disposition_found',
                      'disposition_found']
    target_values = ["Mediated", "Dismissed", "Defaulted", "Heard"]

    for dummy_column, origin_column, target_value in zip(panel_B_columns, origin_columns, target_values):
        df.loc[:, dummy_column] = np.where(df[origin_column] == target_value, 1, 0)

    # Add case duration and money judgment to Panel B.
    panel_B_columns.append('case_duration')
    panel_B_columns.append('judgment')
    panel_B = df[sorted(panel_B_columns)].describe().T
    panel_B = pd.concat([panel_B], keys=["Panel B: Case Resolution"])

    # Panel C: Defendant and Plaintiff Characteristics
    panel_C_columns = ['hasAttyD', 'isEntityD', 'hasAttyP', 'isEntityP']
    panel_C = df[sorted(panel_C_columns)].describe().T
    panel_C = pd.concat([panel_C], keys=["Panel C: Defendant and Plaintiff Characteristics"])

    # Panel D: Tax Assessment Records From F.Y. Following Eviction Filing
    panel_D_columns = ['TOTAL_VAL', 'BLDG_VAL', 'LAND_VAL', 'OTHER_VAL', 'UNITS']
    panel_D = df[sorted(panel_D_columns)].describe().T
    panel_D = pd.concat([panel_D], keys=["Panel D: Assessor Records From Most Recent Pre-Filing F.Y."])

    # Panel E: Zestimates Around Last Docket Date
    # Get month of the latest docket date for each row and use to grab Zestimates at different times prior to treatment.
    df.loc[:, treatment_date_variable] = pd.to_datetime(df[treatment_date_variable])
    df.loc[:, 'nan'] = np.nan
    panel_E_columns = []
    for i in range(-5, 4):
        # This column contains the year-month which is i years relative to treatment for each property.
        offset_docket_month = (df[treatment_date_variable] + pd.tseries.offsets.DateOffset(years=i)).dt.strftime('%Y-%m').copy()

        # Some year-months will be outside the range of our data.
        # For instance, we do not have Zestimates 2 years post-treatment for evictions which occurred in 2022.
        # For these observations, the offset docket month needs to map to the column of nans we created earlier.
        offset_docket_month.loc[~offset_docket_month.isin(df.columns)] = 'nan'

        # Set column accordingly.
        idx, cols = pd.factorize(offset_docket_month)
        new_col_name = f'zestimate_{i}_years_relative_to_treatment'
        panel_E_columns.append(new_col_name)
        df.loc[:, new_col_name] = df.reindex(cols, axis=1).to_numpy()[np.arange(len(df)), idx]

    panel_E = df[panel_E_columns].describe().T
    panel_E = pd.concat([panel_E], keys=["Panel E: Zestimates Around Last Docket Date"])

    # Concatenate Panels A-E
    summary_statistics = pd.concat([panel_A, panel_B, panel_C, panel_D, panel_E], axis=0)[['50%', 'mean', 'std', 'count']]

    return summary_statistics