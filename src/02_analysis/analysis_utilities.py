"""
Functions useful for analysis.
"""
from os.path import join

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
import figure_utilities
from differences.did.pscore_cal import pscore_mle
from typing import List


def generate_variable_names(analysis: str):
    if analysis == 'zestimate':
        years = [str(year) for year in range(2013, 2023)]
        months = ["0" + str(month) for month in range(1, 10)] + [str(month) for month in range(10, 13)]
        value_vars = ["2012-12"] + [str(year) + "-" + str(month) for year in years for month in months]
        value_vars_zestimate = [value_var + "_zestimate" for value_var in value_vars]
        month_to_int_dictionary = {key: value + 1 for value, key in enumerate(value_vars)}
        int_to_month_dictionary = {key + 1: value for key, value in enumerate(value_vars)}

        to_return = value_vars_zestimate
    elif analysis == 'any_crime_60m':
        # Store list of crime variable names and create dictionaries which map between month variable names to integers.
        years = [str(year) for year in range(2015, 2023)]
        months = ["0" + str(month) for month in range(1, 10)] + [str(month) for month in range(10, 13)]
        value_vars = [str(year) + "-" + str(month) for year in years for month in months]
        value_vars = value_vars[5:]
        value_vars.append('2023-01')
        value_vars_crime = [value_var + f"_{analysis}" for value_var in value_vars]
        month_to_int_dictionary = {key: value + 1 for value, key in enumerate(value_vars)}
        int_to_month_dictionary = {key + 1: value for key, value in enumerate(value_vars)}

        to_return = value_vars_crime
    elif analysis == 'any_crime_90m':
        # Store list of crime variable names and create dictionaries which map between month variable names to integers.
        years = [str(year) for year in range(2015, 2023)]
        months = ["0" + str(month) for month in range(1, 10)] + [str(month) for month in range(10, 13)]
        value_vars = [str(year) + "-" + str(month) for year in years for month in months]
        value_vars = value_vars[5:]
        value_vars.append('2023-01')
        value_vars_crime = [value_var + f"_{analysis}" for value_var in value_vars]
        month_to_int_dictionary = {key: value + 1 for value, key in enumerate(value_vars)}
        int_to_month_dictionary = {key + 1: value for key, value in enumerate(value_vars)}

        to_return = value_vars_crime
    elif analysis == 'any_crime_140m':
        # Store list of crime variable names and create dictionaries which map between month variable names to integers.
        years = [str(year) for year in range(2015, 2023)]
        months = ["0" + str(month) for month in range(1, 10)] + [str(month) for month in range(10, 13)]
        value_vars = [str(year) + "-" + str(month) for year in years for month in months]
        value_vars = value_vars[5:]
        value_vars.append('2023-01')
        value_vars_crime = [value_var + f"_{analysis}" for value_var in value_vars]
        month_to_int_dictionary = {key: value + 1 for value, key in enumerate(value_vars)}
        int_to_month_dictionary = {key + 1: value for key, value in enumerate(value_vars)}

        to_return = value_vars_crime
    elif analysis == 'any_crime_200m':
        # Store list of crime variable names and create dictionaries which map between month variable names to integers.
        years = [str(year) for year in range(2015, 2023)]
        months = ["0" + str(month) for month in range(1, 10)] + [str(month) for month in range(10, 13)]
        value_vars = [str(year) + "-" + str(month) for year in years for month in months]
        value_vars = value_vars[5:]
        value_vars.append('2023-01')
        value_vars_crime = [value_var + f"_{analysis}" for value_var in value_vars]
        month_to_int_dictionary = {key: value + 1 for value, key in enumerate(value_vars)}
        int_to_month_dictionary = {key + 1: value for key, value in enumerate(value_vars)}

        to_return = value_vars_crime
    elif analysis == 'any_crime_280m':
        # Store list of crime variable names and create dictionaries which map between month variable names to integers.
        years = [str(year) for year in range(2015, 2023)]
        months = ["0" + str(month) for month in range(1, 10)] + [str(month) for month in range(10, 13)]
        value_vars = [str(year) + "-" + str(month) for year in years for month in months]
        value_vars = value_vars[5:]
        value_vars.append('2023-01')
        value_vars_crime = [value_var + f"_{analysis}" for value_var in value_vars]
        month_to_int_dictionary = {key: value + 1 for value, key in enumerate(value_vars)}
        int_to_month_dictionary = {key + 1: value for key, value in enumerate(value_vars)}

        to_return = value_vars_crime
    elif analysis == 'any_crime_400m':
        # Store list of crime variable names and create dictionaries which map between month variable names to integers.
        years = [str(year) for year in range(2015, 2023)]
        months = ["0" + str(month) for month in range(1, 10)] + [str(month) for month in range(10, 13)]
        value_vars = [str(year) + "-" + str(month) for year in years for month in months]
        value_vars = value_vars[5:]
        value_vars.append('2023-01')
        value_vars_crime = [value_var + f"_{analysis}" for value_var in value_vars]
        month_to_int_dictionary = {key: value + 1 for value, key in enumerate(value_vars)}
        int_to_month_dictionary = {key + 1: value for key, value in enumerate(value_vars)}

        to_return = value_vars_crime
    else:
        raise ValueError("Unrecognized argument for parameter analysis.")

    return to_return, month_to_int_dictionary, int_to_month_dictionary


def prepare_df(df: pd.DataFrame, analysis: str, treatment_date_variable: str, pre_treatment_covariates: List[str],
               value_vars: List[str], month_to_int_dictionary):
    if analysis == 'zestimate':
        pass
    elif analysis == 'any_crime_60m':
        pass
    elif analysis == 'any_crime_90m':
        pass
    elif analysis == 'any_crime_140m':
        pass
    elif analysis == 'any_crime_200m':
        pass
    elif analysis == 'any_crime_280m':
        pass
    elif analysis == 'any_crime_400m':
        pass
    else:
        raise ValueError("Unrecognized argument for parameter analysis.")

    # Store treatment month and year variables.
    treatment_month_variable = treatment_date_variable.replace("date", "month")
    treatment_year_variable = treatment_date_variable.replace("date", "year")

    # Reshape from wide to long.
    df = pd.melt(df,
                 id_vars=['case_number', treatment_month_variable, treatment_year_variable,
                          'judgment_for_plaintiff'] + pre_treatment_covariates,
                 value_vars=value_vars, var_name='month', value_name=analysis)
    df = df.sort_values(by=['case_number', 'month'])

    # Convert months from string format to integer format.
    df.loc[:, 'month'] = df['month'].str.replace(f"_{analysis}", '', regex=False).replace(month_to_int_dictionary)
    df.loc[:, treatment_month_variable] = df[treatment_month_variable].replace(month_to_int_dictionary)
    # Set treatment month to 0 for untreated observations.
    never_treated_mask = (df['judgment_for_plaintiff'] == 0)
    df.loc[never_treated_mask, treatment_month_variable] = np.NaN
    df.loc[never_treated_mask, treatment_year_variable] = np.NaN

    # Generate numeric version of case_number.
    df.loc[:, 'case_number_numeric'] = df['case_number'].astype('category').cat.codes.astype(int)

    # Set index.
    df = df.set_index(['case_number_numeric', 'month'])
    return df


def add_missing_indicators(df: pd.DataFrame, missing_variables: List[str], pre_treatment_covariates: List[str]):
    for missing_variable in missing_variables:
        df.loc[:, missing_variable] = df[missing_variable].fillna(0)
        df.loc[:, missing_variable + '_missing'] = np.where(df['rent_twobed2015'] == 0, 1, 0)
        pre_treatment_covariates.append(missing_variable + '_missing')


def test_balance(df: pd.DataFrame, analysis: str, covariate_exploration_df: pd.DataFrame, output_directory: str):
    # Store pre-treatment panel names.
    pre_treatment_panels = ['Panel A: Pre-treatment Outcomes',
                            'Panel B: Census Tract Characteristics',
                            'Panel C: Case Initiation',
                            'Panel D: Defendant and Plaintiff Characteristics']

    # Balance only on covariates which predict the outcome variable.
    predicts_outcome_mask = covariate_exploration_df.iloc[:, 0] <= 0.05

    # Build treatment mean columns.
    pd.options.mode.chained_assignment = None
    treatment_means, variable_display_names_dict = produce_summary_statistics(
        df.copy().loc[df['judgment_for_plaintiff'] == 1, :], 'file_date')
    treatment_means = treatment_means.loc[pre_treatment_panels, :]
    # Do not include rows corresponding to other outcomes in the covariate exploration table.
    outcomes = ['zestimate', 'any_crime_60m', 'any_crime_90m',
                'any_crime_140m', 'any_crime_200m',
                'any_crime_280m', 'any_crime_400m']  # Create list of all outcomes.
    outcomes.remove(analysis)  # Remove the one which is being currently studied.
    unneeded_outcomes = outcomes
    for unneeded_outcome in unneeded_outcomes:  # For each outcome not currently being studied...
        # Drop related variables from the summary statistics table.
        treatment_means = treatment_means.drop(f'twenty_seventeen_{unneeded_outcome}', level=1, axis=0)
        treatment_means = treatment_means.drop(f'pre_treatment_change_in_{unneeded_outcome}', level=1, axis=0)

    treatment_means = (treatment_means.loc[predicts_outcome_mask, 'mean']
                       .rename("Cases Won by Plaintiff"))
    # Save pre-treatment covariates for use in D.R. DiD estimator.
    pre_treatment_covariates = treatment_means.index.get_level_values(1).tolist()
    pd.options.mode.chained_assignment = 'warn'

    # Calculate propensity scores for every observation.
    df.loc[:, 'propensity_score'] = pd.Series(
        pscore_mle(df.dropna(subset=pre_treatment_covariates)['judgment_for_plaintiff'],
                   exog=df.dropna(subset=pre_treatment_covariates)[pre_treatment_covariates],
                   weights=None)[0])  # Calculate propensity scores.

    # Build unweighted columns.
    difference_unadjusted = []
    p_values_unadjusted = []
    for covariate in pre_treatment_covariates:
        result = smf.ols(formula=f"{covariate} ~ judgment_for_plaintiff",
                         data=df,
                         missing='drop').fit()
        difference_unadjusted.append(result.params.loc['judgment_for_plaintiff'])
        p_values_unadjusted.append(result.pvalues.loc['judgment_for_plaintiff'])
    difference_unadjusted = pd.Series(difference_unadjusted, index=treatment_means.index)
    p_values_unadjusted = pd.Series(p_values_unadjusted, index=treatment_means.index)
    unweighted_columns = pd.concat([difference_unadjusted, p_values_unadjusted], axis=1)
    unweighted_columns.columns = ['Unweighted', '\\emph{p}']

    # Build propensity score-weighted columns.
    differences_propensity_score_adjusted = []
    p_values_propensity_score_adjusted = []
    for covariate in pre_treatment_covariates:
        propensity_score_adjusted_result = smf.ols(formula=f"{covariate} ~ judgment_for_plaintiff + propensity_score",
                                                   data=df,
                                                   missing='drop').fit()
        differences_propensity_score_adjusted.append(
            propensity_score_adjusted_result.params.loc['judgment_for_plaintiff'])
        p_values_propensity_score_adjusted.append(
            propensity_score_adjusted_result.pvalues.loc['judgment_for_plaintiff'])
    differences_propensity_score_adjusted = pd.Series(differences_propensity_score_adjusted,
                                                      index=treatment_means.index)
    p_values_propensity_score_adjusted = pd.Series(p_values_propensity_score_adjusted, index=treatment_means.index)
    propensity_score_weighted_columns = pd.concat(
        [differences_propensity_score_adjusted, p_values_propensity_score_adjusted], axis=1)
    propensity_score_weighted_columns.columns = ['Weighted', '\\emph{p}']

    difference_columns = pd.concat([unweighted_columns, propensity_score_weighted_columns], axis=1)
    table_columns = [treatment_means, difference_columns]
    balance_table = pd.concat(table_columns, axis=1, keys=['', 'Difference in Cases Won by Defendant'])

    balance_table = balance_table.rename(index=variable_display_names_dict)
    # TODO: Figure out how to make the outermost index labels wrap in LaTeX so that I don't have to shorten the panel labels below!
    balance_table = balance_table.rename(index={"Panel A: Pre-treatment Outcomes": "Panel A",
                                                "Panel B: Census Tract Characteristics": "Panel B",
                                                "Panel C: Case Initiation": "Panel C",
                                                "Panel D: Defendant and Plaintiff Characteristics": "Panel D"})

    # Export to LaTeX.
    filename = join(output_directory, "balance_table.tex")
    latex = (balance_table
             .style
             .format(thousands=",",
                     na_rep='',
                     formatter={('', 'Cases Won by Plaintiff'): "{:,.2f}",
                                ('Difference in Cases Won by Defendant', 'Unweighted'): "{:,.2f}",
                                ('Difference in Cases Won by Defendant', '\\emph{p}'): "{:,.2f}",
                                ('Difference in Cases Won by Defendant', 'Weighted'): "{:,.2f}",
                                ('', 'N'): "{:,.0f}"})
             .format_index("\\textit{{{}}}", escape="latex", axis=0, level=0)
             .format_index("\\textit{{{}}}", escape="latex", axis=1, level=0)
             .to_latex(None,
                       column_format="llccccc",
                       hrules=True,
                       multicol_align='c',
                       clines="skip-last;data")).replace("{*}", "{3cm}")
    latex = latex.split("\\\\\n")
    latex.insert(1, "\\cline{4-7}\n")
    latex = "\\\\\n".join(latex)
    with open(filename, 'w') as file:
        file.write(latex)
    return balance_table, pre_treatment_covariates


def select_controls(df: pd.DataFrame, analysis: str, output_directory: str):
    """Choose covariates to include in D.R. model. TODO: Update documentation"""
    # Set column names of the covariate exploration table and check that specified analyis is valid.
    if analysis == 'zestimate':
        covariate_exploration_table_columns = ["Zestimate, Dec. 2022", "Plaintiff Victory"]
    elif analysis == 'any_crime_60m':
        covariate_exploration_table_columns = ["Any Crime Incidents Within 60m, Dec. 2022", "Plaintiff Victory"]
    elif analysis == 'any_crime_90m':
        covariate_exploration_table_columns = ["Any Crime Incidents Within 90m, Dec. 2022", "Plaintiff Victory"]
    elif analysis == 'any_crime_140m':
        covariate_exploration_table_columns = ["Any Crime Incidents Within 140m, Dec. 2022", "Plaintiff Victory"]
    elif analysis == 'any_crime_200m':
        covariate_exploration_table_columns = ["Any Crime Incidents Within 200m, Dec. 2022", "Plaintiff Victory"]
    elif analysis == 'any_crime_280m':
        covariate_exploration_table_columns = ["Any Crime Incidents Within 280m, Dec. 2022", "Plaintiff Victory"]
    elif analysis == 'any_crime_400m':
        covariate_exploration_table_columns = ["Any Crime Incidents Within 400m, Dec. 2022", "Plaintiff Victory"]
    else:
        raise ValueError("Unrecognized argument for parameter analysis.")

    # Run produce summary statistics on the DataFrame to get names of column names of potential pre-treatment covaiates.
    summary_statistics, variable_display_names_dict = produce_summary_statistics(df, 'file_date')

    # Do not include rows corresponding to other outcomes in the covariate exploration table.
    outcomes = ['zestimate', 'any_crime_60m', 'any_crime_90m',
                'any_crime_140m', 'any_crime_200m',
                'any_crime_280m', 'any_crime_400m']  # Create list of all outcomes.
    outcomes.remove(analysis)  # Remove the one which is being currently studied.
    unneeded_outcomes = outcomes
    for unneeded_outcome in unneeded_outcomes:  # For each outcome not currently being studied...
        # Drop related variables from the summary statistics table.
        summary_statistics = summary_statistics.drop(f'twenty_seventeen_{unneeded_outcome}', level=1, axis=0)
        summary_statistics = summary_statistics.drop(f'pre_treatment_change_in_{unneeded_outcome}', level=1, axis=0)

    # Store independent and dependent variables.
    independent_variable = 'judgment_for_plaintiff'
    dependent_variable = f'final_month_of_panel_{analysis}'

    # Must create alias columns for Patchy to work.
    df.loc[:, dependent_variable] = df[f'2022-12_{analysis}']

    # Build covariate exploration table.
    pre_treatment_panels = ["Panel A: Pre-treatment Outcomes",
                            "Panel B: Census Tract Characteristics",
                            "Panel C: Case Initiation",
                            "Panel D: Defendant and Plaintiff Characteristics"]
    summary_statistics = summary_statistics.loc[pre_treatment_panels, :]
    potential_covariates = summary_statistics.index.get_level_values(1)
    p_values = []
    for potential_covariate in potential_covariates:
        # Get p-value from regression of outcome on covariates.
        p_y = (smf.ols(formula=f"{dependent_variable} ~ {potential_covariate}",
                       data=df,
                       missing='drop')
        .fit().pvalues.loc[potential_covariate])
        # Get p-value from regression of treatment on covariates.
        p_x = (smf.ols(formula=f"{independent_variable} ~ {potential_covariate}",
                       data=df,
                       missing='drop')
        .fit().pvalues.loc[potential_covariate])
        p_values.append((p_y, p_x))
    covariate_exploration_df = (pd.DataFrame(p_values,
                                             columns=covariate_exploration_table_columns,
                                             index=summary_statistics.index))
    covariate_exploration_df = pd.concat([covariate_exploration_df], axis=1, keys=['Dependent Variable'])
    covariate_exploration_df.index = covariate_exploration_df.index.set_names(['',
                                                                               '\\emph{Independent Variable}'])
    # Export to LaTeX.
    filename = join(output_directory, "pre_treatment_covariate_tests.tex")
    latex = (covariate_exploration_df
             .rename(index=variable_display_names_dict)
             .style
             .format(formatter="{:0.2f}")
             .format_index("\\textit{{{}}}", escape="latex", axis=0, level=0)
             .format_index("\\textit{{{}}}", escape="latex", axis=1, level=0)
             .to_latex(None,
                       column_format="llcc",
                       hrules=True,
                       multicol_align='c',
                       clines="skip-last;data")
             .replace("{*}", "{3cm}"))
    latex = latex.split("\\\\\n")
    latex.insert(1, "\\cline{3-4}\n")
    latex = "\\\\\n".join(latex)
    with open(filename, 'w') as file:
        file.write(latex)
    return covariate_exploration_df


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
    # Panel A: Pre-treatment Outcomes
    outcomes = ['zestimate', 'any_crime_60m', 'any_crime_90m',
                'any_crime_140m', 'any_crime_200m',
                'any_crime_280m', 'any_crime_400m']  # Create list of all outcomes.
    panel_A_columns = []
    for outcome in outcomes:
        # Create alias column for Patchy.
        df.loc[:, f'twenty_seventeen_{outcome}'] = df[f'2017-01_{outcome}']
        panel_A_columns.append(f'twenty_seventeen_{outcome}')
        panel_A_columns.append(f'pre_treatment_change_in_{outcome}')
        df.loc[:, f'pre_treatment_change_in_{outcome}'] = df[f'2019-01_{outcome}'] - df[f'2017-01_{outcome}']
    panel_A = df[panel_A_columns].describe().T
    panel_A = pd.concat([panel_A], keys=["Panel A: Pre-treatment Outcomes"])

    # Panel B: Census Tract Characteristics
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

    # Panel F: Post-treatment Outcomes
    # Get month of the latest docket date for each row and use to grab outcomes at different times prior to treatment.
    df.loc[:, treatment_date_variable] = pd.to_datetime(df[treatment_date_variable])
    df.loc[:, 'nan'] = np.nan
    panel_F_columns = []
    start = 1
    stop = 2
    for i in range(start, stop + 1):
        for outcome in outcomes:
            # This column contains the year-month which is i years relative to treatment for each property.
            offset_docket_month = (df[treatment_date_variable] + pd.tseries.offsets.DateOffset(years=i)) \
                                      .dt.strftime('%Y-%m').copy() + "_" + outcome

            # Some year-months will be outside the range of our data.
            # For instance, we do not have outcomes 2 years post-treatment for evictions which occurred in 2022.
            # For these observations, the offset docket month needs to map to the column of nans we created earlier.
            offset_docket_month.loc[~offset_docket_month.isin(df.columns)] = 'nan'

            idx, cols = pd.factorize(offset_docket_month)
            if i < 0:
                new_col_name = f'{outcome}_minus{-1 * i}_years_relative_to_treatment'
            else:
                new_col_name = f'{outcome}_{i}_years_relative_to_treatment'
            panel_F_columns.append(new_col_name)
            df.loc[:, new_col_name] = df.reindex(cols, axis=1).to_numpy()[np.arange(len(df)), idx]
    panel_F = df[panel_F_columns].describe().T
    panel_F = pd.concat([panel_F], keys=["Panel F: Post-treatment Outcomes"])

    # Concatenate Panels A-E
    summary_statistics = pd.concat([panel_A, panel_B, panel_C, panel_D, panel_E, panel_F],
                                   axis=0)[['mean', '50%', 'std', 'count']]

    # TODO: Update display names at the end of project!
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
                                   'twenty_seventeen_zestimate': 'Zestimate, Jan. 2017',
                                   'twenty_seventeen_crimes': 'Crimes reported, Jan. 2017',
                                   'change_in_crimes': 'Change in Crimes Reported, Jan. 2017 to Jan. 2019',
                                   'twenty_eighteen': 'Zestimate, Jan. 2018',
                                   'change_in_zestimates': 'Change in Zestimate, Jan. 2017 to Jan. 2019',
                                   'zestimate_1_years_relative_to_treatment': "Zestimate one year after filing date",
                                   'zestimate_2_years_relative_to_treatment': "Zestimate two years after filing date",
                                   'crimes_1_years_relative_to_treatment': "Crimes one year after filing date",
                                   'crimes_2_years_relative_to_treatment': "Crimes two years after filing date",
                                   }
    return summary_statistics, variable_display_names_dict
