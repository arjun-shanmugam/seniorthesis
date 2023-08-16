class Variables:
    outcomes = ['group_0_crimes_150m', 'group_1_crimes_150m', 'group_2_crimes_150m',
                'group_0_crimes_200m', 'group_1_crimes_200m', 'group_2_crimes_200m',
                'group_0_crimes_250m', 'group_1_crimes_250m', 'group_2_crimes_250m',
                'group_0_crimes_300m', 'group_1_crimes_300m', 'group_2_crimes_300m',
                'group_0_crimes_350m', 'group_1_crimes_350m', 'group_2_crimes_350m',
                'group_0_crimes_400m', 'group_1_crimes_400m', 'group_2_crimes_400m',
                'group_0_crimes_450m', 'group_1_crimes_450m', 'group_2_crimes_450m',
                'group_0_crimes_500m', 'group_1_crimes_500m', 'group_2_crimes_500m']


class Analysis:
    MAIN_RESULTS_RADIUS = 300
    MINIMUM_PRE_PERIOD = -5
    MAXIMUM_POST_PERIOD = 36


class Colors:
    P1 = "#29B6A4"
    P2 = "#FAA523"
    P3 = "#003A4F"
    P4 = "#7F4892"
    P5 = "#A4CE4E"
    P6 = "#2B8F43"
    P7 = "#0073A2"
    P8 = "#E54060"
    P9 = "#FFD400"
    P10 = "#6BBD45"

    SUMMARY_STATISTICS_COLOR = 'black'
    LABELING_COLOR = 'grey'
    TREATMENT_COLOR = 'red'
    CONTROL_COLOR = 'blue'


class Text:
    DEFAULT_DECIMAL_PLACES = 3
