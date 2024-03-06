"""
Functions useful for analysis.
"""
from os.path import join
import pandas as pd
import statsmodels.formula.api as smf
import statsmodels.api as sm
import constants
from differences.did.pscore_cal import pscore_mle
from panel_utilities import get_value_variable_names, prepare_df_for_DiD
import rpy2.robjects.packages as rpackages
import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri, Formula, numpy2ri
from rpy2.robjects.packages import importr
from rpy2.robjects.vectors import StrVector
from typing import List
import numpy as np


def run_sensitivity_analysis_in_R(df: pd.DataFrame,
                 analysis: str,
                 treatment_date_variable: str, 
                 pre_treatment_covariates: List[str],
                 value_vars: List[str],
                 period_to_int_dictionary: dict,
                 event_time: int,
                 type_and_vector_string: str,                
                 min_e: int=constants.Analysis.MINIMUM_PRE_PERIOD,
                 max_e: int=constants.Analysis.MAXIMUM_POST_PERIOD):
    # Reshape DF to long
    long_df = prepare_df_for_DiD(df=df,
                                 analysis=analysis,
                                 treatment_date_variable=treatment_date_variable,
                                 pre_treatment_covariates=pre_treatment_covariates,
                                 value_vars=value_vars,
                                 period_to_int_dictionary=period_to_int_dictionary)

    # install necessary R packages
    utils = rpackages.importr('utils')
    utils.chooseCRANmirror(ind=1)
    packnames = ['did']
    names_to_install = [x for x in packnames if not rpackages.isinstalled(x)]
    if len(names_to_install) > 0:
        utils.install_packages(StrVector(names_to_install))
        
    # run DiD
    
    base = importr('base')
    did = importr('did')
    if len(pre_treatment_covariates) == 0:
        formula = Formula("~1")
    else:
        formula = Formula(f"~{'+'.join(pre_treatment_covariates)}")
        
    with (robjects.default_converter + pandas2ri.converter).context():
        long_df_R = robjects.conversion.get_conversion().py2rpy(long_df)
        robjects.r('''
                   options(warn=1)
                   att_gt_result <- att_gt(yname = "{yname}",
                                           tname = "month",
                                           idname = "case_number_numeric",
                                           gname = "{tname}",
                                           xformla = {xformla},
                                           data = {data},
                                           cband=FALSE,
                                           bstrap=FALSE,
                                           base_period = "universal",
                                           pl=TRUE,
                                           cores=10)
                   att_e_result  <- aggte(att_gt_result,
                                          type = "dynamic", 
                                          cband=FALSE,
                                          bstrap=FALSE,
                                          min_e={min_e},
                                          max_e={max_e})
                   att_e_result_tidy <- tidy(att_e_result)
               
                   '''.format(**{'yname': analysis,
                                 'tname': treatment_date_variable,
                                 'min_e': min_e,
                                 'max_e': max_e,
                                 'xformla': formula.r_repr(), 
                                 'data': long_df_R.r_repr()}))
    
    robjects.r('''

            honest_did <- function(...) UseMethod("honest_did")
            honest_did.AGGTEobj <- function(es,
            e          = 0,
            type       = c("smoothness", "relative_magnitude"),
            gridPoints = 100,
            ...) {

            type <- match.arg(type)

            # Make sure that user is passing in an event study
            if (es$type != "dynamic") {
            stop("need to pass in an event study")
            }

            # Check if used universal base period and warn otherwise
            if (es$DIDparams$base_period != "universal") {
            stop("Use a universal base period for honest_did")
            }

            # Recover influence function for event study estimates
            es_inf_func <- es$inf.function$dynamic.inf.func.e

            # Recover variance-covariance matrix
            n <- nrow(es_inf_func)
            V <- t(es_inf_func) %*% es_inf_func / n / n

            # Check time vector is consecutive with referencePeriod = -1
            referencePeriod <- -1
            consecutivePre  <- !all(diff(es$egt[es$egt <= referencePeriod]) == 1)
            consecutivePost <- !all(diff(es$egt[es$egt >= referencePeriod]) == 1)
            if ( consecutivePre | consecutivePost ) {
            msg <- "honest_did expects a time vector with consecutive time periods;"
            msg <- paste(msg, "please re-code your event study and interpret the results accordingly.", sep="\n")
            stop(msg)
            }

            # Remove the coefficient normalized to zero
            hasReference <- any(es$egt == referencePeriod)
            if ( hasReference ) {
            referencePeriodIndex <- which(es$egt == referencePeriod)
            V    <- V[-referencePeriodIndex,-referencePeriodIndex]
            beta <- es$att.egt[-referencePeriodIndex]
            } else {
            beta <- es$att.egt
            }

            nperiods <- nrow(V)
            npre     <- sum(1*(es$egt < referencePeriod))
            npost    <- nperiods - npre
            if ( !hasReference & (min(c(npost, npre)) <= 0) ) {
            if ( npost <= 0 ) {
            msg <- "not enough post-periods"
            } else {
            msg <- "not enough pre-periods"
            }
            msg <- paste0(msg, " (check your time vector; note honest_did takes -1 as the reference period)")
            stop(msg)
            }
            baseVec1 <- HonestDiD::basisVector(index=(e+1),size=npost)
            orig_ci  <- HonestDiD::constructOriginalCS(betahat        = beta,
            sigma          = V,
            numPrePeriods  = npre,
            numPostPeriods = npost,
            l_vec          = baseVec1)

            if (type=="relative_magnitude") {
            robust_ci <- HonestDiD::createSensitivityResults_relativeMagnitudes(betahat        = beta,
                                     sigma          = V,
                                     numPrePeriods  = npre,
                                     numPostPeriods = npost,
                                     l_vec          = baseVec1,
                                     gridPoints     = gridPoints,
                                     ...)

            } else if (type == "smoothness") {
            robust_ci <- HonestDiD::createSensitivityResults(betahat        = beta,
                  sigma          = V,
                  numPrePeriods  = npre,
                  numPostPeriods = npost,
                  l_vec          = baseVec1,   
                  ...)
            }

            return(list(robust_ci=robust_ci, orig_ci=orig_ci, type=type))
            }
            
            
            sensitivity_results <- honest_did(att_e_result,
                                              e=to_replace,
                                              type_and_vector_parameters)
            '''.replace("to_replace", str(event_time)).replace("type_and_vector_parameters", type_and_vector_string))

    return robjects.r['sensitivity_results'][0]
    
    

def run_did_in_R(df: pd.DataFrame,
                 analysis: str,
                 treatment_date_variable: str, 
                 pre_treatment_covariates: List[str],
                 value_vars: List[str],
                 period_to_int_dictionary: dict,
                 min_e: int=constants.Analysis.MINIMUM_PRE_PERIOD,
                 max_e: int=constants.Analysis.MAXIMUM_POST_PERIOD):
    # Reshape DF to long
    long_df = prepare_df_for_DiD(df=df,
                                 analysis=analysis,
                                 treatment_date_variable=treatment_date_variable,
                                 pre_treatment_covariates=pre_treatment_covariates,
                                 value_vars=value_vars,
                                 period_to_int_dictionary=period_to_int_dictionary)

    # install necessary R packages
    utils = rpackages.importr('utils')
    utils.chooseCRANmirror(ind=1)
    packnames = ['did']
    names_to_install = [x for x in packnames if not rpackages.isinstalled(x)]
    if len(names_to_install) > 0:
        utils.install_packages(StrVector(names_to_install))
        
    # run DiD
    
    base = importr('base')
    did = importr('did')
    if len(pre_treatment_covariates) == 0:
        formula = Formula("~1")
    else:
        formula = Formula(f"~{'+'.join(pre_treatment_covariates)}")
        
    with (robjects.default_converter + pandas2ri.converter).context():
        long_df_R = robjects.conversion.get_conversion().py2rpy(long_df)
        robjects.r('''
                   options(warn=1)
                   att_gt_result <- att_gt(yname = "{yname}",
                                           tname = "month",
                                           idname = "case_number_numeric",
                                           gname = "{tname}",
                                           xformla = {xformla},
                                           data = {data},
                                           base_period = "universal",
                                           
                                           pl=TRUE,
                                           cores=10)
                   att_e_result  <- aggte(att_gt_result,
                                          type = "dynamic",
                                          min_e={min_e},
                                          max_e={max_e},
                                          bstrap=FALSE,
                                          cband=FALSE)
                   att_e_result_tidy <- tidy(att_e_result)
               
                   '''.format(**{'yname': analysis,
                                 'tname': treatment_date_variable,
                                 'min_e': min_e,
                                 'max_e': max_e,
                                 'xformla': formula.r_repr(), 
                                 'data': long_df_R.r_repr()}))
        att_e_result_tidy = robjects.conversion.get_conversion().rpy2py(robjects.r['att_e_result_tidy'])
        att_e_result = robjects.r['att_e_result']
    return att_e_result_tidy, att_e_result  


def run_event_study(df: pd.DataFrame, treatment_date_variable: str, analysis: str):
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

    # Restrict to the treatment relative months we care about
    df = df.loc[df['treatment_relative_month'].between(constants.Analysis.MINIMUM_PRE_PERIOD, constants.Analysis.MAXIMUM_POST_PERIOD), :]
    
    y = df['value']
    x_variables = []
    x_variables.append(df['judgment_for_plaintiff'])
    month_dummies = pd.get_dummies(df['treatment_relative_month'], prefix='month')
    month_dummies = month_dummies.drop(columns='month_-1')
    x_variables.append(month_dummies)
    month_times_treatment_indicator_dummies = (month_dummies
                                               .mul(df['judgment_for_plaintiff'], axis=0))
    month_times_treatment_indicator_dummies.columns = [col + "_X_treatment_indicator" for col in
                                                       month_times_treatment_indicator_dummies.columns]
    x_variables.append(month_times_treatment_indicator_dummies)
    X = pd.concat(x_variables, axis=1)


    return sm.OLS(y, sm.add_constant(X)).fit()

def test_balance(df: pd.DataFrame, analysis: str, output_directory: str = None):
    # Store pre-treatment panel names.
    pre_treatment_panels = ['Panel A: Pre-Treatment Crime Levels',
                            'Panel B: Census Tract Characteristics',
                            'Panel C: Case Initiation']
    # Build treatment mean columns.
    pd.options.mode.chained_assignment = None
    treatment_means = produce_summary_statistics(
        df.copy().loc[df['judgment_for_plaintiff'] == 0, :])
    treatment_means = treatment_means.loc[pre_treatment_panels, :]
    # Do not include rows corresponding to other outcomes in the covariate exploration table.
    outcomes = constants.Variables.outcomes.copy()  # Create list of all outcomes.

    outcomes.remove(analysis)  # Remove the one which is being currently studied.
    unneeded_outcomes = outcomes
    for unneeded_outcome in unneeded_outcomes:  # For each outcome not currently being studied...
        # Drop related variables from the summary statistics table.
        treatment_means = treatment_means.drop(f'total_twenty_seventeen_{unneeded_outcome}', level=1, axis=0)
        treatment_means = treatment_means.drop(f'month_neg_twelve_{unneeded_outcome}', level=1, axis=0)
        treatment_means = treatment_means.drop(f'month_neg_six_{unneeded_outcome}', level=1, axis=0)
        continue
    
    treatment_means = (treatment_means.loc[:, 'mean']
                       .rename("Cases Won by Defendant"))
    
    # Save pre-treatment covariates for use in D.R. DiD estimator.
    pre_treatment_covariates = treatment_means.index.get_level_values(1).tolist()
    pd.options.mode.chained_assignment = 'warn'

    # Calculate propensity scores for every observation.
    pscores = (sm.Logit(df['judgment_for_plaintiff'],
                             exog=df[constants.Variables.pre_treatment_covariates_to_include])
                    .fit()
                    .predict(df[constants.Variables.pre_treatment_covariates_to_include]))
    print(constants.Variables.pre_treatment_covariates_to_include)
    df.loc[:, 'propensity_score'] = pd.Series(pscores, index=df.index)
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
    unweighted_columns.columns = ['Unweighted', '\\emph{p}']

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
    propensity_score_weighted_columns.columns = ['Weighted', '\\emph{p}']

    difference_columns = pd.concat([unweighted_columns, propensity_score_weighted_columns], axis=1)
    table_columns = [treatment_means, difference_columns]
    balance_table = pd.concat(table_columns, axis=1, keys=['', 'Difference in Cases Won by Plaintiff'])

    balance_table = balance_table.rename(index=constants.Variables.variable_display_names_dict)
    # TODO: Figure out how to make the outermost index labels wrap in LaTeX so that I don't have to shorten the panel labels below!
    balance_table = balance_table.rename(index={"Panel A: Pre-Treatment Crime Levels": "Panel A",
                                                "Panel B: Census Tract Characteristics": "Panel B",
                                                "Panel C: Case Initiation": "Panel C",
                                                "Panel D: Defendant and Plaintiff Characteristics": "Panel D"})

    
    if output_directory is not None:
        # Export to LaTeX.
        filename = join(output_directory, "balance_table.tex")
        latex = (balance_table
                 .rename(index=constants.Variables.variable_display_names_dict)
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
                           clines="skip-last;data")).replace("{*}", "{3cm}")
        latex = latex.split("\\\\\n")
        latex.insert(1, "\\cline{4-7}\n")
        latex = "\\\\\n".join(latex)

        with open(filename, 'w') as file:
            file.write(latex)
    return balance_table



def produce_summary_statistics(df: pd.DataFrame):
    """

    :param df:
    :param treatment_date_variable:
    :return:
    """
    # Panel A: Total Incidents in 2017
    outcomes = constants.Variables.outcomes.copy()  # Create list of all outcomes.
    panel_A_columns = []
    for outcome in outcomes:
        panel_A_columns.append(f'total_twenty_seventeen_{outcome}')
        panel_A_columns.append(f'month_neg_twelve_{outcome}')
        panel_A_columns.append(f'month_neg_six_{outcome}')
        
    panel_A = df[panel_A_columns].describe().T
    panel_A = pd.concat([panel_A], keys=["Panel A: Pre-Treatment Crime Levels"])
    

    # Panel B: Census Tract Characteristics
    panel_B_columns = ['med_hhinc2016', 'popdensity2010',
                       'poor_share2010', 'share_white2010']
    panel_B = df[sorted(panel_B_columns)].describe().T
    panel_B = pd.concat([panel_B], keys=["Panel B: Census Tract Characteristics"])

    # Panel C: Case Initiaton
    panel_C_columns = ['non_payment', 'hasAttyD']
    panel_C = df[sorted(panel_C_columns)].describe().T
    panel_C = pd.concat([panel_C], keys=["Panel C: Case Initiation"])

    # Panel D: Case Resolution
    panel_D_columns = ['dismissed', 'defaulted', 'heard', 'case_duration', 'judgment']
    panel_D = df[sorted(panel_D_columns)].describe().T
    panel_D = pd.concat([panel_D], keys=["Panel D: Case Resolution"])


    # Concatenate Panels A-E
    summary_statistics = pd.concat([panel_A, panel_B, panel_C, panel_D],
                                   axis=0)[['mean', 'std', '50%']]




    return summary_statistics
