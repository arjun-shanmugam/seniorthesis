"""
Functions useful for analysis.
"""
from os.path import join

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import figure_utilities


def aggregate_by_event_time_and_plot(att_gt,
                                     output_folder: str,
                                     filename: str,
                                     start_period: int,
                                     end_period: int,
                                     title: str,
                                     treatment_month_variable: str,
                                     df: pd.DataFrame):
    # Get event study-aggregated ATT(t)s.
    results_df = att_gt.aggregate('event')
    results_df = results_df.loc[start_period:end_period]
    results_df.columns = results_df.columns.droplevel().droplevel()

    # Plot event study-style plot of ATTs.
    fig, (ax, ax2) = plt.subplots(2, 1, sharex=True, height_ratios=[4, 1], layout='constrained')
    x = results_df.index
    y = results_df['ATT']
    y_upper = results_df['upper']
    y_lower = results_df['lower']
    ax.set_ylabel("ATT")
    ax.set_title(title)
    figure_utilities.plot_labeled_vline(ax, x=0, text="Treatment Month", color='black', linestyle='-',
                                        text_y_location_normalized=0.95)
    figure_utilities.plot_scatter_with_shaded_errors(ax,
                                                     x.values,
                                                     y.values,
                                                     y_upper.values,
                                                     y_lower.values,
                                                     point_color='black',
                                                     error_color='white',
                                                     edge_color='grey',
                                                     edge_style='--',
                                                     zorder=1)
    figure_utilities.plot_labeled_hline(ax, y=0, text="", color='black', linestyle='-', zorder=6)

    # Plot sample size at each event-time.
    df_copy = df.copy().reset_index()
    df_copy.loc[:, 'event_time'] = df_copy['month'] - df_copy[treatment_month_variable]
    cases_per_year = df_copy.groupby('event_time')['case_number'].nunique().loc[start_period:end_period]
    x = cases_per_year.index
    y = cases_per_year.values
    ax2.plot(x, y, color='black')
    ax2.set_xlabel("Month Relative to Treatment")
    ax2.set_ylabel("Number of Units")
    ax2.grid(True)
    ax2.set_title("Sample Size")

    plt.show()
    figure_utilities.save_figure_and_close(fig, join(output_folder, filename))


def aggregate_by_time_and_plot(att_gt, int_to_month_dictionary: dict, output_folder: str, filename: str, title: str):
    # Get time-aggregated ATTs.
    results_df = att_gt.aggregate('time')

    # Plot event study-style plot of ATTs.
    fig, ax = plt.subplots()
    results_df = results_df.rename(index=int_to_month_dictionary)
    x = results_df.index
    y = results_df.iloc[:, 0]
    y_upper = results_df.iloc[:, 3]
    y_lower = results_df.iloc[:, 2]
    ax.set_xlabel("Month")
    ax.set_ylabel("ATT")
    ax.set_title(title)
    figure_utilities.plot_labeled_vline(ax, x=results_df.index.tolist()[0], text="Earliest Treatment Date in Sample",
                                        color='black', linestyle='-',
                                        text_y_location_normalized=0.95)
    figure_utilities.plot_scatter_with_shaded_errors(ax,
                                                     x.values,
                                                     y.values,
                                                     y_upper.values,
                                                     y_lower.values,
                                                     point_color='black',
                                                     error_color='white',
                                                     edge_color='grey',
                                                     edge_style='--',
                                                     zorder=1)
    figure_utilities.plot_labeled_hline(ax, y=0, text="", color='black', linestyle='-')
    ax.set_xticks(range(0, len(x), 12))
    plt.show()
    figure_utilities.save_figure_and_close(fig, join(output_folder, filename))


def produce_summary_statistics(df: pd.DataFrame, treatment_date_variable: str):
    """

    :param df:
    :param treatment_date_variable:
    :return:
    """
    # Panel A: Case Initiaton
    panel_A_columns = ['for_cause', 'no_cause', 'non_payment']
    origin_columns = ['initiating_action', 'initiating_action', 'initiating_action']
    target_values = [("SP Summons and Complaint - Cause", "SP Transfer - Cause"),
                     ("SP Summons and Complaint - No Cause", "SP Transfer- No Cause"),
                     ("SP Summons and Complaint - Non-payment of Rent", "SP Transfer - Non-payment of Rent")]

    for dummy_column, origin_column, target_value in zip(panel_A_columns, origin_columns, target_values):
        df.loc[:, dummy_column] = np.where((df[origin_column].str.contains(target_value[0])) |
                                           (df[origin_column].str.contains(target_value[1])),
                                           1,
                                           0)

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

    """
    # Panel D: Tax Assessment Records From F.Y. Following Eviction Filing
    panel_D_columns = ['TOTAL_VAL', 'BLDG_VAL', 'LAND_VAL', 'OTHER_VAL']
    panel_D = df[sorted(panel_D_columns)].describe().T
    panel_D = pd.concat([panel_D], keys=["Panel D: Assessor Records"])
    """

    # Panel E: Census Tract Characteristics
    panel_E_columns = ['med_hhinc2016', 'popdensity2010', 'share_white2010']
    panel_E = df[sorted(panel_E_columns)].describe().T
    panel_E = pd.concat([panel_E], keys=["Panel E: Census Tract Characteristics"])

    # Panel F: Zestimates Before Filing
    df.loc[:, 'change_in_zestimates_2018_to_2019'] = df['2019-01'] - df['2018-01']
    panel_F_columns = ['2018-01', 'change_in_zestimates_2018_to_2019']
    panel_F = df[panel_F_columns].describe().T
    panel_F = pd.concat([panel_F], keys=["Panel F: Pretreatment Zestimates"])

    # Concatenate Panels A-E
    summary_statistics = pd.concat([panel_A, panel_B, panel_C, panel_E, panel_F], axis=0)[
        ['mean', '50%', 'std', 'count']]

    variable_display_names_dict = {'for_cause': "For cause", 'no_cause': "No cause",
                                   'non_payment': "Non-payment of rent",  # Panel A
                                   'case_duration': "Case duration", 'defaulted': "Case defaulted",
                                   'heard': "Case heard",  # Panel B
                                   'judgment': "Money judgment", 'mediated': "Case mediated",
                                   'dismissed': 'Case dismised',  # Panel B
                                   'hasAttyD': "Defendant has an attorney", 'hasAttyP': "Plaintiff has an attorney",
                                   # Panel C
                                   'isEntityD': "Defendant is an entity", 'isEntityP': "Plaintiff is an entity",
                                   # Panel C
                                   'TOTAL_VAL': "Total property value", 'BLDG_VAL': "Building value",
                                   'LAND_VAL': "Land value",  # Panel D
                                   'OTHER_VAL': "Other value",  # Panel D
                                   'med_hhinc2016': 'Median household income (2016)',
                                   'popdensity2010': 'Population density (2010)',  # Panel E
                                   'share_white2010': 'Portion white (2010)',  # Panel E
                                   '2018-01': 'Jan. 2018', 'change_in_zestimates_2018_to_2019': 'Change from Jan. 2018 to Jan. 2019',
                                   'zestimate_-5_years_relative_to_treatment': "Five years before filing date",
                                   'zestimate_-4_years_relative_to_treatment': "Four years before filing date",
                                   'zestimate_-3_years_relative_to_treatment': "Three years before filing date",
                                   'zestimate_-2_years_relative_to_treatment': "Two years before filing date",
                                   'zestimate_-1_years_relative_to_treatment': "One year before filing date",
                                   'zestimate_0_years_relative_to_treatment': "Filing date",
                                   'zestimate_1_years_relative_to_treatment': "One year after filing date",
                                   'zestimate_2_years_relative_to_treatment': "Two years after filing date",
                                   'zestimate_3_years_relative_to_treatment': "Three years after filing date"}
    return summary_statistics, variable_display_names_dict
