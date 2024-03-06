"""
Functions useful for analysis.
"""
from os.path import join
import pandas as pd
import statsmodels.formula.api as smf
import statsmodels.api as sm
import constants
from differences.did.pscore_cal import pscore_mle
from panel_utilities import get_value_variable_names


def run_event_study(df: pd.DataFrame, treatment_date_variable: str, analysis: str, use_calendar_months=False):
    # Reshape to long
    triplet = get_value_variable_names(df, analysis)
    weekly_value_vars_crime, month_to_int_dictionary, int_to_month_dictionary = triplet
    df = pd.melt(df,
                 id_vars=['case_number',
                          'judgment_for_plaintiff',
                          treatment_date_variable],
                 value_vars=weekly_value_vars_crime,
                 var_name='month')
    df.loc[:, 'month'] = df['month'].str[:7]  # Drop "_group_0_crimes_500m" from the end of each month

    # Replace months with integers
    df.loc[:, [treatment_date_variable, 'month']] = df[[treatment_date_variable, 'month']].replace(month_to_int_dictionary)

    # Calculate crime levels during each month relative to treatment, separately for treatment and control gropu
    df.loc[:, 'treatment_relative_month'] = df['month'] - df[treatment_date_variable]

    # Create column containing calendar month
    df.loc[:, 'calendar_month'] = df['month'].replace(int_to_month_dictionary)

    if use_calendar_months:
        y = df['value']
        x_variables = []
        x_variables.append(df['judgment_for_plaintiff'])
        month_dummies = pd.get_dummies(df['calendar_month'], prefix='month', drop_first=True)
        x_variables.append(month_dummies)
        month_times_treatment_indicator_dummies = (month_dummies
                                                   .mul(df['judgment_for_plaintiff'], axis=0))
        month_times_treatment_indicator_dummies.columns = [col + "_X_treatment_indicator" for col in
                                                           month_times_treatment_indicator_dummies.columns]
        x_variables.append(month_times_treatment_indicator_dummies)
        X = pd.concat(x_variables, axis=1)

        omitted_period = df['calendar_month'].iloc[0]
        omitted_period_control_mean = df.loc[(df['calendar_month'] == omitted_period) &
                                             (df['judgment_for_plaintiff'] == 0), 'value'].mean()
    else:
        y = df['value']
        x_variables = []
        x_variables.append(df['judgment_for_plaintiff'])
        month_dummies = pd.get_dummies(df['treatment_relative_month'], prefix='month', drop_first=True)
        x_variables.append(month_dummies)
        month_times_treatment_indicator_dummies = (month_dummies
                                                   .mul(df['judgment_for_plaintiff'], axis=0))
        month_times_treatment_indicator_dummies.columns = [col + "_X_treatment_indicator" for col in
                                                           month_times_treatment_indicator_dummies.columns]
        x_variables.append(month_times_treatment_indicator_dummies)
        X = pd.concat(x_variables, axis=1)

        omitted_period = df['treatment_relative_month'].min()
        omitted_period_control_mean = df.loc[(df['treatment_relative_month'] == omitted_period) &
                                             (df['judgment_for_plaintiff'] == 0), 'value'].mean()

    return sm.OLS(y, X).fit(), omitted_period_control_mean

def test_balance(df: pd.DataFrame, analysis: str, output_directory: str = None):
    # Store pre-treatment panel names.
    pre_treatment_panels = ['Panel A: Pre-treatment Outcomes',
                            'Panel B: Census Tract Characteristics',
                            'Panel C: Case Initiation']
    # Build treatment mean columns.
    pd.options.mode.chained_assignment = None
    treatment_means, variable_display_names_dict = produce_summary_statistics(
        df.copy().loc[df['judgment_for_plaintiff'] == 1, :])
    treatment_means = treatment_means.loc[pre_treatment_panels, :]
    # Do not include rows corresponding to other outcomes in the covariate exploration table.
    outcomes = constants.Variables.outcomes.copy()  # Create list of all outcomes.

    outcomes.remove(analysis)  # Remove the one which is being currently studied.
    unneeded_outcomes = outcomes
    for unneeded_outcome in unneeded_outcomes:  # For each outcome not currently being studied...
        # Drop related variables from the summary statistics table.
        treatment_means = treatment_means.drop(f'total_twenty_eighteen_{unneeded_outcome}', level=1, axis=0)
        treatment_means = treatment_means.drop(f'pre_treatment_change_in_{unneeded_outcome}', level=1, axis=0)
        treatment_means = treatment_means.drop(f'relative_pre_treatment_change_in_{unneeded_outcome}', level=1, axis=0)

    treatment_means = (treatment_means.loc[:, 'mean']
                       .rename("Cases Won by Plaintiff (1)"))
    # Save pre-treatment covariates for use in D.R. DiD estimator.
    pre_treatment_covariates = treatment_means.index.get_level_values(1).tolist()
    controls = pre_treatment_covariates.copy()
    pd.options.mode.chained_assignment = 'warn'

    # Calculate propensity scores for every observation.
    df.loc[:, 'propensity_score'] = pd.Series(
        pscore_mle(df.dropna(subset=controls)['judgment_for_plaintiff'],
                   exog=df.dropna(subset=controls)[controls],
                   weights=None)[0], index=df.index)  # Calculate propensity scores.
    df.loc[:, 'weight'] = df['propensity_score'] / (1 - df['propensity_score'])
    df.loc[df['judgment_for_plaintiff'] == 1, 'weight'] = 1

    # Build unweighted columns.
    difference_unadjusted = []
    p_values_unadjusted = []
    for covariate in pre_treatment_covariates:
        result = smf.ols(formula=f"{covariate} ~ judgment_for_plaintiff",
                         data=df,
                         missing='drop').fit()

        difference_unadjusted.append(result.params.loc['judgment_for_plaintiff'])
        p_values_unadjusted.append(result.pvalues.loc['judgment_for_plaintiff'])
    difference_unadjusted = pd.Series(difference_unadjusted , index=treatment_means.index)
    p_values_unadjusted = pd.Series(p_values_unadjusted, index=treatment_means.index)
    unweighted_columns = pd.concat([difference_unadjusted, p_values_unadjusted], axis=1)
    unweighted_columns.columns = ['Unweighted (2)', '\\emph{p} (3)']

    # Build propensity score-weighted columns.
    differences_propensity_score_adjusted = []
    p_values_propensity_score_adjusted = []
    for covariate in pre_treatment_covariates:
        propensity_score_adjusted_result = sm.WLS.from_formula(f"{covariate} ~ judgment_for_plaintiff",
                                                               data=df,
                                                               missing='drop',
                                                               weights=df['weight']).fit()
        differences_propensity_score_adjusted.append(
            propensity_score_adjusted_result.params.loc['judgment_for_plaintiff'])
        p_values_propensity_score_adjusted.append(
            propensity_score_adjusted_result.pvalues.loc['judgment_for_plaintiff'])
    differences_propensity_score_adjusted = pd.Series(differences_propensity_score_adjusted,
                                                      index=treatment_means.index)
    p_values_propensity_score_adjusted = pd.Series(p_values_propensity_score_adjusted, index=treatment_means.index)
    propensity_score_weighted_columns = pd.concat(
        [differences_propensity_score_adjusted, p_values_propensity_score_adjusted], axis=1)
    propensity_score_weighted_columns.columns = ['Weighted (4)', '\\emph{p} (5)']

    difference_columns = pd.concat([unweighted_columns, propensity_score_weighted_columns], axis=1)
    table_columns = [treatment_means, difference_columns]
    balance_table = pd.concat(table_columns, axis=1, keys=['', 'Difference in Cases Won by Defendant'])

    balance_table = balance_table.rename(index=variable_display_names_dict)
    # TODO: Figure out how to make the outermost index labels wrap in LaTeX so that I don't have to shorten the panel labels below!
    balance_table = balance_table.rename(index={"Panel A: Pre-treatment Outcomes": "Panel A",
                                                "Panel B: Census Tract Characteristics": "Panel B",
                                                "Panel C: Case Initiation": "Panel C",
                                                "Panel D: Defendant and Plaintiff Characteristics": "Panel D"})
    if output_directory is not None:
        # Export to LaTeX.
        filename = join(output_directory, "balance_table.tex")
        latex = (balance_table
                 .rename(index=variable_display_names_dict)
                 .style
                 .format(thousands=",",
                         na_rep='',
                         formatter="{:,.2f}")
                 .format_index("\\textit{{{}}}", escape="latex", axis=0, level=0)
                 .format_index("\\textit{{{}}}", escape="latex", axis=1, level=0)
                 .to_latex(None,
                           column_format="llccccc",
                           hrules=True,
                           multicol_align='c',
                           clines="skip-last;data")).replace("{*}", "{2cm}")
        latex = latex.split("\\\\\n")
        latex.insert(1, "\\cline{4-7}\n")
        latex = "\\\\\n".join(latex)

        with open(filename, 'w') as file:
            file.write(latex)
    return balance_table, controls


def select_controls(df: pd.DataFrame, treatment_date_variable: str, analysis: str, output_directory: str = None):
    """Choose covariates to include in D.R. model."""
    covariate_exploration_table_columns = ["Change in Crime Incidents, April 2019-March 2020",
                                           "Treated Property"]  # TODO: Add column naming logic.

    # Run produce summary statistics on the DataFrame to get names of column names of potential pre-treatment covaiates.
    summary_statistics, variable_display_names_dict = produce_summary_statistics(df, 'latest_docket_date')

    # Do not include rows corresponding to other outcomes in the covariate exploration table.
    outcomes = constants.Variables.outcomes.copy()  # Create list of all outcomes.
    outcomes.remove(analysis)  # Remove the one which is being currently studied.
    unneeded_outcomes = outcomes
    for unneeded_outcome in unneeded_outcomes:  # For each outcome not currently being studied...
        # Drop related variables from the summary statistics table.
        summary_statistics = summary_statistics.drop(f'total_twenty_eighteen_{unneeded_outcome}', level=1, axis=0)
        summary_statistics = summary_statistics.drop(f'pre_treatment_change_in_{unneeded_outcome}', level=1, axis=0)
        summary_statistics = summary_statistics.drop(f'relative_pre_treatment_change_in_{unneeded_outcome}', level=1, axis=0)

    # Store independent and dependent variables.
    independent_variable = 'judgment_for_plaintiff'
    dependent_variable = f'change_in_{analysis}_over_all_treated_weeks'
    if "month" in treatment_date_variable:
        last_week_in_panel = '2020-03'
    else:
        last_week_in_panel = '2023-00'
    first_treated_week = df[treatment_date_variable].sort_values().iloc[0]
    df.loc[:, dependent_variable] = df[f'{last_week_in_panel}_{analysis}'] - df[f'{first_treated_week}_{analysis}']

    # Build covariate exploration table.
    pre_treatment_panels = ["Panel A: Pre-treatment Outcomes",
                            "Panel B: Census Tract Characteristics",
                            "Panel C: Case Initiation"]
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
    if output_directory != None:
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


def produce_summary_statistics(df: pd.DataFrame):
    """

    :param df:
    :param treatment_date_variable:
    :return:
    """
    # Panel A: Pre-treatment Outcomes
    outcomes = constants.Variables.outcomes.copy()  # Create list of all outcomes.
    panel_A_columns = []
    for outcome in outcomes:
        panel_A_columns.append(f'total_twenty_eighteen_{outcome}')
        panel_A_columns.append(f'pre_treatment_change_in_{outcome}')
        panel_A_columns.append(f'relative_pre_treatment_change_in_{outcome}')
    panel_A = df[panel_A_columns].describe().T
    panel_A = pd.concat([panel_A], keys=["Panel A: Pre-treatment Outcomes"])

    # Panel B: Census Tract Characteristics
    panel_B_columns = ['med_hhinc2016', 'popdensity2010', 'frac_coll_plus2010', 'job_density_2013',
                       'poor_share2010', 'share_white2010']
    panel_B = df[sorted(panel_B_columns)].describe().T
    panel_B = pd.concat([panel_B], keys=["Panel B: Census Tract Characteristics"])

    # Panel C: Case Initiaton
    panel_C_columns = ['for_cause', 'non_payment', 'no_cause', 'hasAttyD', 'hasAttyP']
    panel_C = df[sorted(panel_C_columns)].describe().T
    panel_C = pd.concat([panel_C], keys=["Panel C: Case Initiation"])

    # Panel E: Case Resolution
    panel_D_columns = ['dismissed', 'defaulted', 'heard', 'case_duration', 'judgment']
    panel_D = df[sorted(panel_D_columns)].describe().T
    panel_D = pd.concat([panel_D], keys=["Panel D: Case Resolution"])


    # Concatenate Panels A-E
    summary_statistics = pd.concat([panel_A, panel_B, panel_C, panel_D],
                                   axis=0)[['mean', '50%', 'std', 'count']]

    variable_display_names_dict = {'total_twenty_eighteen_group_0_crimes_250m': "Total Incidents, 2018",
                                   'relative_pre_treatment_change_in_group_0_crimes_250m': "Total Incidents, Month -12 - Total Incidents, Month 0",
                                   'pre_treatment_change_in_group_0_crimes_250m': "Total Incidents, 2019 - Total Incidents, 2018",

                                   'frac_coll_plus2010': "Bachelor's degree, 2010",
                                   'job_density_2013': "Job density, 2013",
                                   'med_hhinc2016': "Median household income, 2016",
                                   'poor_share2010': "Poverty rate, 2010",
                                   'popdensity2010': "Population density, 2010",
                                   'share_white2010': "Share white, 2010",

                                   'for_cause': "Filing for cause",
                                   'no_cause': "Filing without cause",
                                   'non_payment': "Filing for nonpayment",
                                   'hasAttyD': "Defendant has attorney",
                                   'hasAttyP': "Plaintiff has attorney",

                                   "case_duration": "Case duration",
                                   "defaulted": "Judgment by default",
                                   'dismissed': "Case dismissed",
                                   "heard": "Case heard",
                                   "judgment": "Money judgment",

                                   }

    return summary_statistics, variable_display_names_dict
