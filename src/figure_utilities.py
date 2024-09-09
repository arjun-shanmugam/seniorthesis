"""
Defines useful helper functions which create nice figures
using Matplotlib.

All functions written here receive a Matplotlib Axes instance as an argument
and plot on that axis. They do not interact with Matplotlib Figure instances.
Thus, the user must instantiate all subplots and close all figures
separately.
"""
from os.path import join

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.axes import Axes
import pandas as pd
from matplotlib.colors import to_rgba

import constants
import matplotlib.transforms as transforms

def add_column_numbers(table: pd.DataFrame):
    df = table.copy()
    arrays = [df.columns.get_level_values(n) for n in range(df.columns.nlevels)] + [[f'({column_number})' for column_number in range(1, 1 + len(df.columns))]]
    df.columns = pd.MultiIndex.from_arrays(arrays)

    return df

def build_pretrends_table(att_e_result_tidy: pd.DataFrame):
    pretrend_test = (att_e_result_tidy.
                 loc[att_e_result_tidy['event.time'].between(constants.Analysis.MINIMUM_PRE_PERIOD,
                                                             -2,
                                                             inclusive='both'), :]
                 .loc[:, ['event.time', 'estimate', 'conf.low', 'conf.high', 'std.error']]
                 .rename(columns={'event.time': '$e$',
                                  'estimate': 'ATT(e)'}))
    pretrend_test.loc[:, '95\% C.I.'] = ("(" +
                                        pretrend_test['conf.low'].round(2).astype(str) +
                                        ", " +
                                        pretrend_test['conf.high'].round(2).astype(str) +
                                        ")")
    pretrend_test.loc[:, 'C.I. includes zero?'] = "Yes"
    pretrend_test = pretrend_test.drop(columns=['conf.low', 'conf.high', 'std.error'])
    pretrend_test = pretrend_test.set_index('$e$')
    return pretrend_test

def plot_att_e(att_e: pd.DataFrame,
               overall_att: float,
               overall_att_se: float,
               ax: Axes,
               title: str="",
               start_period: int=constants.Analysis.MINIMUM_PRE_PERIOD,
               end_period: int=constants.Analysis.MAXIMUM_POST_PERIOD,
               xlabel: str="Month Relative to Treatment",
               ylabel: str="ATT"):
    # CIs for event time -1 are currently NaN, replace them with zeroes 
    att_e.loc[att_e['event.time'] == -1, ['conf.low', 'conf.high', 'point.conf.low', 'point.conf.high']] = [0, 0, 0, 0]
    
    att_e = att_e.loc[att_e['event.time'].between(start_period, end_period, inclusive='both'), :]
    x = att_e['event.time']
    y = att_e['estimate']
    y_upper = att_e['conf.high']
    y_lower = att_e['conf.low']
    
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_xticks([-12, -6, 0, 6, 12, 18, 24, 30, 36])
    ax.axvline(0, color='black')
    plot_scatter_with_shaded_errors(ax,
                                    x,
                                    y,
                                    y_upper,
                                    y_lower,
                                    point_color='black',
                                    error_color='white',
                                    edge_color='grey',
                                    edge_style='--',
                                    zorder=1)
    plot_labeled_hline(ax, y=0, text="", color='black', linestyle='-', zorder=6)
    

    
     # Label graph with average treatment effect across positive relative periods.
    point_estimate = round(overall_att, 2)
    se = round(overall_att_se, 2)
    label = f"Avg.\nPost-Treatment\nATT: {point_estimate}\n(SE: {se})"
    


def aggregate_by_event_time_and_plot(att_gt,
                                     start_period: int,
                                     end_period: int,
                                     title: str,
                                     ax: Axes):
    # Get event study-aggregated ATT(t)s.
    results_df = att_gt.aggregate('event')
    results_df = results_df.loc[start_period:end_period]
    results_df.columns = results_df.columns.droplevel().droplevel()

    # Plot event study-style plot of ATTs.
    x = results_df.index
    y = results_df['ATT']
    y_upper = results_df['upper'].where(results_df['upper'].notna(), y)
    y_lower = results_df['lower'].where(results_df['lower'].notna(), y)
    ax.set_ylabel("ATT")
    ax.set_title(title)
    ax.set_xlabel("Month Relative to Treatment")
    ax.set_xticks([-12, -6, 0, 12, 24, 36])
    plot_labeled_vline(ax, x=0, text="Treatment Month", color='black', linestyle='-',
                       text_y_location_normalized=0.95)

    plot_scatter_with_shaded_errors(ax,
                                    x,
                                    y,
                                    y_upper,
                                    y_lower,
                                    point_color='black',
                                    error_color='white',
                                    edge_color='grey',
                                    edge_style='--',
                                    zorder=1)
    plot_labeled_hline(ax, y=0, text="", color='black', linestyle='-', zorder=6)

    # Label graph with average treatment effect across positive relative periods.
    average_post_treatment_att = att_gt.aggregate('event', overall=True)
    point_estimate = round(average_post_treatment_att['EventAggregationOverall'].iloc[0, 0], 2)
    se = round(average_post_treatment_att['EventAggregationOverall'].iloc[0, 1], 2)
    label = f"Avg.\nPost-Treatment\nATT: {point_estimate}\n(SE: {se})"

    plot_labeled_hline(ax, y=point_estimate, size='x-small', text=label, color='black', linestyle='--', zorder=6,
                       text_x_location_normalized=1.085)



def save_figure_and_close(figure: plt.Figure,
                          filename: str,
                          bbox_inches: str = 'tight'):
    """
    Helper function to save and close a provided figure.
    :param figure: Figure to save and close.
    :param filename: Location on disk to save figure.
    :param bbox_inches: How to crop figure before saving.
    """
    figure.savefig(filename, bbox_inches=bbox_inches)
    plt.show()
    plt.close(figure)


def plot_scatter_with_shaded_errors(ax: plt.Axes,
                                    x: pd.Series,
                                    y: pd.Series,
                                    y_upper: pd.Series,
                                    y_lower: pd.Series,
                                    label: str = "",
                                    point_color: str = constants.Colors.P3,
                                    point_size: float = 2,
                                    marker: str = 'o',
                                    error_color: str = constants.Colors.P1,
                                    error_opacity: float = 0.5,
                                    edge_color: str = constants.Colors.P1,
                                    edge_style: str = '-',
                                    zorder=None):
    """

    :param label:
    :param marker:
    :param ax:
    :param x: 
    :param y: 
    :param y_upper: 
    :param y_lower: 
    :param point_color: 
    :param point_size: 
    :param error_color: 
    :param error_opacity: 
    :param zorder: 
    """

    if zorder is None:
        ax.scatter(x, y, color=point_color, marker=marker, s=point_size, label=label)
        ax.fill_between(x, y_upper, y_lower, color=error_color, alpha=error_opacity, edgecolor=edge_color,
                        linestyle=edge_style)
        ax.axvline(x=x.min(), color='white')
        ax.axvline(x=x.max(), color='white')
    else:
        ax.fill_between(x, y_upper, y_lower, color=error_color, alpha=error_opacity, edgecolor=edge_color,
                        linestyle=edge_style, zorder=zorder)
        ax.axvline(x=x.min(), color='white', zorder=zorder + 1)
        ax.axvline(x=x.max(), color='white', zorder=zorder + 1)
        ax.scatter(x, y, color=point_color, marker=marker, s=point_size, label=label, zorder=zorder + 2)

def plot_labeled_hline(ax: Axes,
                       y: float,
                       text: str,
                       color: str = constants.Colors.P10,
                       linestyle: str = '--',
                       text_x_location_normalized: float = 0.85,
                       size='small',
                       zorder: int = 10):
    """
    Plots a horizontal line labeled with text on the provided Axes.
    :param ax: An Axes instance on which to plot.
    :param y: The y-coordinate of the horizontal line.
    :param text: The text with which to label the horizontal line.
    :param color: The color of the horizontal line and of the text.
    :param linestyle: The style of the horizontal line.
    :param text_x_location_normalized: The x-coordinate of the text
            as a portion of the total length of the X-axis.
    :param size: The size of the text.
    :param zorder: Passed as Matplotlib zorder argument in ax.text() call.
    """

    # Plot horizontal line.
    ax.axhline(y=y,
               c=color,
               linestyle=linestyle)

    # Create blended transform for coordinates.
    transform = transforms.blended_transform_factory(ax.transAxes,  # X should be axes coordinates (between 0 and 1)
                                                     ax.transData)  # Y should be in data coordinates

    ax.text(x=text_x_location_normalized,
            y=y,
            s=text,
            c=color,
            size=size,
            horizontalalignment='center',
            verticalalignment='center',
            bbox=dict(facecolor='white', lw=1, ec='black'),
            transform=transform,
            zorder=zorder)


def plot_labeled_vline(ax: Axes,
                       x: float,
                       text: str,
                       color: str = constants.Colors.P10,
                       linestyle: str = '--',
                       text_y_location_normalized: float = 0.85,
                       size='small',
                       zorder: int = 10):
    """

    :param ax: An Axes instance on which to plot.
    :param x: The x-coordinate of the vertical line.
    :param text: The text with which to label the vertical line.
    :param color: The color of the vertical line and of the text.
    :param linestyle: The style of the vertical line.
    :param text_y_location_normalized: The y-coordinate of the
            text as a portion of the total length of the y-axis.
    :param size: The size of the text.
    :param zorder: Passed as Matplotlib zorder argument in ax.text() call.
    """

    # Plot vertical line.
    ax.axvline(x=x,
               c=color,
               linestyle=linestyle)

    # Create blended transform for coordinates.
    transform = transforms.blended_transform_factory(ax.transData,  # X should be in data coordinates
                                                     ax.transAxes)  # Y should be in axes coordinates (between 0 and 1)

    # Plot text
    ax.text(x=x,
            y=text_y_location_normalized,
            s=text,
            c=color,
            size=size,
            horizontalalignment='center',
            verticalalignment='center',
            bbox=dict(facecolor='white', lw=1, ec='black'),
            transform=transform,
            zorder=zorder)

