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
    # Panel A: Pre-treatment Zestimates
    df.loc[:, 'twenty_seventeen'] = df['2017-01']
    df.loc[:, 'change_in_zestimates'] = df['2019-01'] - df['2017-01']
    panel_A_columns = ['twenty_seventeen', 'change_in_zestimates']
    panel_A = df[panel_A_columns].describe().T
    panel_A = pd.concat([panel_A], keys=["Panel A: Pre-treatment Zestimates"])

    # Panel B: Census Tract Characteristics
    # TODO: RE-ADD 'mean_commutetime2000' TO THIS LIST AFTER RE-RUNNING 06_merge.py
    panel_B_columns = ['med_hhinc2016', 'popdensity2010', 'share_white2010', 'frac_coll_plus2010', 'job_density_2013',
                        'poor_share2010', 'traveltime15_2010', 'rent_twobed2015']
    panel_B = df[sorted(panel_B_columns)].describe().T
    panel_B = pd.concat([panel_B], keys=["Panel B: Census Tract Characteristics"])

    # Panel C: Case Initiaton
    panel_C_columns = ['for_cause', 'no_cause', 'non_payment']
    origin_columns = ['initiating_action', 'initiating_action', 'initiating_action']
    target_values = [("SP Summons and Complaint - Cause", "SP Transfer - Cause"),
                     ("SP Summons and Complaint - No Cause", "SP Transfer- No Cause"),
                     ("SP Summons and Complaint - Non-payment of Rent", "SP Transfer - Non-payment of Rent")]

    for dummy_column, origin_column, target_value in zip(panel_C_columns, origin_columns, target_values):
        df.loc[:, dummy_column] = np.where((df[origin_column].str.contains(target_value[0])) |
                                           (df[origin_column].str.contains(target_value[1])),
                                           1,
                                           0)
    panel_C = df[sorted(panel_C_columns)].describe().T
    panel_C = pd.concat([panel_C], keys=["Panel C: Case Initiation"])

    # Panel D: Defendant and Plaintiff Characteristics
    panel_D_columns = ['hasAttyD', 'isEntityD', 'hasAttyP', 'isEntityP']
    panel_D = df[sorted(panel_D_columns)].describe().T
    panel_D = pd.concat([panel_D], keys=["Panel D: Defendant and Plaintiff Characteristics"])

    # Panel E: Case Resolution
    panel_E_columns = ['mediated', 'dismissed', 'defaulted', 'heard']
    origin_columns = ['disposition_found', 'disposition_found', 'disposition_found',
                      'disposition_found']
    target_values = ["Mediated", "Dismissed", "Defaulted", "Heard"]

    for dummy_column, origin_column, target_value in zip(panel_E_columns, origin_columns, target_values):
        df.loc[:, dummy_column] = np.where(df[origin_column] == target_value, 1, 0)
    # Add case duration and money judgment to Panel E.
    panel_E_columns.append('case_duration')
    panel_E_columns.append('judgment')
    panel_E = df[sorted(panel_E_columns)].describe().T
    panel_E = pd.concat([panel_E], keys=["Panel E: Case Resolution"])

    # Panel F: Post-treatment Zestimates
    # Get month of the latest docket date for each row and use to grab Zestimates at different times prior to treatment.
    df.loc[:, treatment_date_variable] = pd.to_datetime(df[treatment_date_variable])
    df.loc[:, 'nan'] = np.nan
    panel_F_columns = []
    start = 0
    stop = 4
    for i in range(start, stop):
        # This column contains the year-month which is i years relative to treatment for each property.
        offset_docket_month = (df[treatment_date_variable] + pd.tseries.offsets.DateOffset(years=i)).dt.strftime(
            '%Y-%m').copy()

        # Some year-months will be outside the range of our data.
        # For instance, we do not have Zestimates 2 years post-treatment for evictions which occurred in 2022.
        # For these observations, the offset docket month needs to map to the column of nans we created earlier.
        offset_docket_month.loc[~offset_docket_month.isin(df.columns)] = 'nan'

        # Set column accordingly.
        idx, cols = pd.factorize(offset_docket_month)
        if i < 0:
            new_col_name = f'zestimate_minus{-1*i}_years_relative_to_treatment'
        else:
            new_col_name = f'zestimate_{i}_years_relative_to_treatment'
        panel_F_columns.append(new_col_name)
        df.loc[:, new_col_name] = df.reindex(cols, axis=1).to_numpy()[np.arange(len(df)), idx]
    panel_F = df[panel_F_columns].describe().T
    panel_F = pd.concat([panel_F], keys=["Panel F: Post-treatment Zestimates"])




    # Concatenate Panels A-E
    summary_statistics = pd.concat([panel_A, panel_B, panel_C, panel_D, panel_E, panel_F],
                                   axis=0)[['mean', '50%', 'std', 'count']]

    variable_display_names_dict = {'for_cause': "For cause", 'no_cause': "No cause",
                                   'non_payment': "Non-payment of rent",
                                   'case_duration': "Case duration", 'defaulted': "Case defaulted",
                                   'heard': "Case heard",
                                   'judgment': "Money judgment", 'mediated': "Case mediated",
                                   'dismissed': 'Case dismised',
                                   'hasAttyD': "Defendant has an attorney", 'hasAttyP': "Plaintiff has an attorney",
                                   'isEntityD': "Defendant is an entity", 'isEntityP': "Plaintiff is an entity",
                                   'med_hhinc2016': 'Median household income (2016)',
                                   'popdensity2010': 'Population density (2010)',
                                   'share_white2010': 'Share white (2010)',
                                   'rent_twobed2015': 'Median two bedroom rent (2015)',
                                   'frac_coll_plus2010': 'Share with bachelor\'s degree',
                                   'job_density_2013': 'Jobs per square mile (2010)',
                                   'mean_commutetime2000': 'Mean commute time (2000)',
                                   'traveltime15_2010': 'Share with commute $<$15 minutes (2010)',
                                   'poor_share2010': 'Share below poverty line',
                                   'twenty_seventeen': 'Zestimate, Jan. 2017',
                                   'twenty_eighteen': 'Zestimate, Jan. 2018',
                                   'change_in_zestimates': 'Change, Jan. 2017 to Jan. 2019',
                                   'zestimate_0_years_relative_to_treatment': "At filing date",
                                   'zestimate_1_years_relative_to_treatment': "One year after filing date",
                                   'zestimate_2_years_relative_to_treatment': "Two years after filing date",
                                   'zestimate_3_years_relative_to_treatment': "Three years after filing date"}
    return summary_statistics, variable_display_names_dict
