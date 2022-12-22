/******************************************************************************/
* Filename:   		  main.do
* Project:    		  Senior Thesis
* Author:     		  Arjun Shanmugam
* Date Created:       November 2nd 2022

* This file attempts to run the instrumental variable analysis.
/******************************************************************************/
include "/Users/arjunshanmugam/Documents/GitHub/seniorthesis/src/02_analysis/exploratory_locals.do"

* Run code
do "`code'/summary_statistics.do"
do "`code'/DiD.do"

