"""
Defines useful helper functions which create nice figures
using Matplotlib.

All functions written here receive a Matplotlib Axes instance as an argument
and plot on that axis. They do not interact with Matplotlib Figure instances.
Thus, the user must instantiate all subplots and close all figures
separately.
"""
import numpy as np
from matplotlib.axes import Axes
import matplotlib.pyplot as plt
import pandas as pd
from src.utilities import figure_and_table_constants
from typing import List
import matplotlib.transforms as transforms

def plot_pie_chart(ax: Axes,
                   x: pd.Series,
                   title: str,
                   colors=None,
                   explode_amount=0.05):
    # Produce series where index gives labels and values give counts.
    x = x.astype(str)  # Convert to string so that we can sort Series which contain both numerics and alphabetic strings.
    N = len(x)
    counts = x.groupby(x).count().sort_index()
    labels = counts.index.tolist()

    # Choose color scheme.
    if colors is None:
        if len(labels) <= 10:
            # Use pre-specified colors.
            colors = [figure_and_table_constants.Colors.P1,
                      figure_and_table_constants.Colors.P3,
                      figure_and_table_constants.Colors.P6,
                      figure_and_table_constants.Colors.P7,
                      figure_and_table_constants.Colors.P2,
                      figure_and_table_constants.Colors.P4,
                      figure_and_table_constants.Colors.P5,
                      figure_and_table_constants.Colors.P8,
                      figure_and_table_constants.Colors.P9,
                      figure_and_table_constants.Colors.P10][:len(labels)]
        else:
            # Let Matplotlib auto-assign colors.
            colors = None

    # Plot pie chart.
    ax.pie(counts,
           labels=labels,
           colors=colors,
           explode=[explode_amount]*len(labels),
           autopct='%2.2f%%')
    ax.set_title(f'{title} (N={N})')

def plot_labeled_hline(ax: Axes,
                       y: float,
                       text: str,
                       c: str = figure_and_table_constants.Colors.P10,
                       linestyle: str = '--',
                       text_x_location_normalized: float = 0.85,
                       size='small',
                       zorder: int = 10):
    """
    Plots a horizontal line labeled with text on the provided Axes.
    :param ax: An Axes instance on which to plot.
    :param y: The y-coordinate of the horizontal line.
    :param text: The text with which to label the horizontal line.
    :param c: The color of the horizontal line and of the text.
    :param linestyle: The style of the horizontal line.
    :param text_x_location_normalized: The x-coordinate of the text
            as a portion of the total length of the X-axis.
    :param size: The size of the text.
    :param zorder: Passed as Matplotlib zorder argument in ax.text() call.
    """

    # Plot horizontal line.
    ax.axhline(y=y,
               c=c,
               linestyle=linestyle)

    # Create blended transform for coordinates.
    transform = transforms.blended_transform_factory(ax.transAxes,  # X should be axes coordinates (between 0 and 1)
                                                     ax.transData)  # Y should be in data coordinates

    ax.text(x=text_x_location_normalized,
            y=y,
            s=text,
            c=c,
            size=size,
            horizontalalignment='center',
            verticalalignment='center',
            bbox=dict(facecolor='white', lw=1, ec='black'),
            transform=transform,
            zorder=zorder)


def plot_labeled_vline(ax: Axes,
                       x: float,
                       text: str,
                       c: str = figure_and_table_constants.Colors.P10,
                       linestyle: str = '--',
                       text_y_location_normalized: float = 0.85,
                       size='small',
                       zorder: int = 10):
    """

    :param ax: An Axes instance on which to plot.
    :param x: The x-coordinate of the vertical line.
    :param text: The text with which to label the vertical line.
    :param c: The color of the vertical line and of the text.
    :param linestyle: The style of the vertical line.
    :param text_y_location_normalized: The y-coordinate of the
            text as a portion of the total length of the y-axis.
    :param size: The size of the text.
    :param zorder: Passed as Matplotlib zorder argument in ax.text() call.
    """

    # Plot vertical line.
    ax.axvline(x=x,
               c=c,
               linestyle=linestyle)

    # Create blended transform for coordinates.
    transform = transforms.blended_transform_factory(ax.transData,  # X should be in data coordinates
                                                     ax.transAxes)  # Y should be in axes coordinates (between 0 and 1)

    # Plot text
    ax.text(x=x,
            y=text_y_location_normalized,
            s=text,
            c=c,
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
                           c=summary_statistics_linecolor,
                           text_y_location_normalized=summary_statistics_text_y_location_normalized)
