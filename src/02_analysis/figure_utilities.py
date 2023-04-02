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

import figure_and_table_constants
import matplotlib.transforms as transforms

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
    fig, ax = plt.subplots(layout='constrained')
    x = results_df.index
    y = results_df['ATT']
    y_upper = results_df['upper']
    y_lower = results_df['lower']
    ax.set_ylabel("ATT")
    ax.set_title(title)
    plot_labeled_vline(ax, x=0, text="Treatment Month", color='black', linestyle='-',
                                        text_y_location_normalized=0.95)
    plot_scatter_with_shaded_errors(ax,
                                                     x.values,
                                                     y.values,
                                                     y_upper.values,
                                                     y_lower.values,
                                                     point_color='black',
                                                     error_color='white',
                                                     edge_color='grey',
                                                     edge_style='--',
                                                     zorder=1)
    plot_labeled_hline(ax, y=0, text="", color='black', linestyle='-', zorder=6)
<<<<<<< HEAD
=======

>>>>>>> 680591684e02dfd7cfa4c25bbe88df310844c4e9
    plt.show()
    save_figure_and_close(fig, join(output_folder, filename))


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
    plot_labeled_vline(ax, x=results_df.index.tolist()[0], text="Earliest Treatment Date in Sample",
                                        color='black', linestyle='-',
                                        text_y_location_normalized=0.95)
    plot_scatter_with_shaded_errors(ax,
                                                     x.values,
                                                     y.values,
                                                     y_upper.values,
                                                     y_lower.values,
                                                     point_color='black',
                                                     error_color='white',
                                                     edge_color='grey',
                                                     edge_style='--',
                                                     zorder=1)
    plot_labeled_hline(ax, y=0, text="", color='black', linestyle='-')
    ax.set_xticks(range(0, len(x), 12))
    plt.show()
    save_figure_and_close(fig, join(output_folder, filename))


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
    plt.close(figure)


def plot_scatter_with_shaded_errors(ax: plt.Axes,
                                    x: pd.Series,
                                    y: pd.Series,
                                    y_upper: pd.Series,
                                    y_lower: pd.Series,
                                    label: str = "",
                                    point_color: str = figure_and_table_constants.Colors.P3,
                                    point_size: float = 2,
                                    marker: str = 'o',
                                    error_color: str = figure_and_table_constants.Colors.P1,
                                    error_opacity: float = 0.5,
                                    edge_color: str = figure_and_table_constants.Colors.P1,
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
        ax.axvline(x=x.min(), color='white', zorder=zorder+1)
        ax.axvline(x=x.max(), color='white', zorder=zorder+1)
        ax.scatter(x, y, color=point_color, marker=marker, s=point_size, label=label, zorder=zorder+2)




def plot_scatter_with_error_bars(ax: plt.Axes,
                                 x: pd.Series,
                                 y: pd.Series,
                                 y_upper: pd.Series,
                                 y_lower: pd.Series,
                                 label: str = "",
                                 point_color: str = figure_and_table_constants.Colors.P3,
                                 point_size: float = 2,
                                 marker: str = 'o',
                                 error_color: str = figure_and_table_constants.Colors.P1,
                                 error_opacity: float = 0.5,
                                 zorder=None):
    """

    :param marker:
    :param label:
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
    yerr = (y_upper - y_lower) / 2
    ecolor = to_rgba(error_color, alpha=error_opacity)
    if zorder is None:
        ax.errorbar(x, y, yerr=yerr, color=point_color, ms=point_size, ecolor=ecolor, fmt=marker, label=label,
                    capsize=3)
    else:
        ax.errorbar(x, y, yerr=yerr, color=point_color, ms=point_size, ecolor=ecolor, fmt=marker, label=label,
                    capsize=3, zorder=zorder)


def plot_labeled_hline(ax: Axes,
                       y: float,
                       text: str,
                       color: str = figure_and_table_constants.Colors.P10,
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
                       color: str = figure_and_table_constants.Colors.P10,
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


def plot_histogram(ax: Axes,
                   x: pd.Series,
                   title: str,
                   xlabel: str,
                   ylabel: str = "Relative Frequency",
                   edgecolor: str = 'black',
                   color: str = figure_and_table_constants.Colors.P1,
                   summary_statistics=None,
                   summary_statistics_linecolor: str = figure_and_table_constants.Colors.P3,
                   summary_statistics_text_y_location_normalized: float = 0.15,
                   decimal_places: int = figure_and_table_constants.Text.DEFAULT_DECIMAL_PLACES,
                   alpha: float = 1,
                   label: str = ""):
    """
    Plot a histogram.
    :param ax: An Axes instance on which to plot.
    :param x: A Series containing data to plot as a histogram.
    :param title: The title for the Axes instance.
    :param xlabel: The xlabel for the Axes instance.
    :param ylabel: The ylabel for the Axes instance.
    :param edgecolor: The edge color of the histogram's bars.
    :param color: The fill color of the histogram's bars.
    :param summary_statistics: The summary statistics to mark on the histogram.
    :param summary_statistics_linecolor: The color of the lines marking the
            summary statistics.
    :param summary_statistics_text_y_location_normalized: The y-coordinate
            of the text labels for summary statistics lines as a portion of
            total y-axis length.
    :param decimal_places: The number of decimal places to include in summary
            statistic labels.
    :param alpha: The opacity of the histogram's bars.
    :param label: The label for the histogram to be used in creating Matplotlib
            legends.
    """

    # Check that requested summary statistics are valid.
    if summary_statistics is None:
        summary_statistics = ['min', 'med', 'max']
    else:
        if len(summary_statistics) > 3:
            raise ValueError("No more than three summary statistics may be requested.")
        for summary_statistic in summary_statistics:
            if (summary_statistic != 'min') and (summary_statistic != 'med') and (summary_statistic != 'max'):
                raise ValueError("When requesting summary statistics, please specify \'min\', \'med\', or \'max\'.")

    # Plot histogram.
    ax.hist(x,
            color=color,
            edgecolor=edgecolor,
            weights=np.ones_like(x) / len(x),
            alpha=alpha,
            label=label)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)

    # Calculate summary statistics.
    statistics = []
    labels = []
    if 'min' in summary_statistics:
        statistics.append(x.min())
        labels.append(f"min: {x.min().round(decimal_places)}")
    if 'med' in summary_statistics:
        statistics.append(x.median())
        labels.append(f"med: {x.median().round(decimal_places)}")
    if 'max' in summary_statistics:
        statistics.append(x.max())
        labels.append(f"max: {x.max().round(decimal_places)}")
    for statistic, label in zip(statistics, labels):
        plot_labeled_vline(ax=ax,
                           x=statistic,
                           text=label,
                           color=summary_statistics_linecolor,
                           text_y_location_normalized=summary_statistics_text_y_location_normalized)
