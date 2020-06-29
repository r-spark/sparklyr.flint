#' Wrapper functions for commonly used summarizer functions
#'
#' R wrapper functions for commonly used Flint summarizer functionalities such as
#' sum and count.
#'
#' @param ts_rdd Timeseries RDD being summarized
#' @param window R expression specifying time windows to be summarized (e.g.,
#'   `in_past("1h")` to summarize data from looking behind 1 hour at each time
#'    point, `in_future("5s")` to summarize data from looking forward 5 seconds
#'    at each time point)
#' @param column Column to be summarized
#' @name summarizers
#'
#' @include sdf_utils.R
#' @include window_exprs.R
NULL

summarize_windows <- function(ts_rdd, window_obj, summarizer, group_by = list()) {
  new_ts_rdd(invoke(ts_rdd, "summarizeWindows", window_obj, summarizer, group_by))
}

summarize <- function(ts_rdd, summarizer, group_by = list()) {
  new_ts_rdd(invoke(ts_rdd, "summarize", summarizer, group_by))
}

#' Evaluate a time window specification and instantiate the corresponding time
#' window object
new_window_obj <- function(sc, window_expr) {
  window_expr$sc <- sc
  rlang::eval_tidy(window_expr)
}

#' Count summarizer
#'
#' Count the total number of rows if no column is specified, or the number of
#' non-null values within the specified column within each time window
#'
#' @inheritParams summarizers
#' @param column If not NULL, then report the number of values in the column
#'   specified that are not NULL or NaN within each time window, and store the
#'   counts in a new column named `<column>_count`.
#'   Otherwise the number of rows within each time window is reported, and
#'   stored in a column named `count`.
#'
#' @export
summarize_count <- function(ts_rdd, window, column = NULL) {
  sc <- spark_connection(ts_rdd)
  window_obj <- new_window_obj(sc, rlang::enexpr(window))

  args <- list(
    sc = sc,
    class = "com.twosigma.flint.timeseries.Summarizers",
    method = "count"
  )
  if (!is.null(column)) args <- append(args, column)

  count_summarizer <- do.call(invoke_static, args)

  summarize_windows(ts_rdd, window_obj, count_summarizer)
}

#' Sum summarizer
#'
#' Compute moving sums on the column specified and store results in a new column
#' named `<column>_sum`
#'
#' @inheritParams summarizers
#'
#' @export
summarize_sum <- function(ts_rdd, window, column) {
  sc <- spark_connection(ts_rdd)
  window_obj <- new_window_obj(sc, rlang::enexpr(window))

  sum_summarizer <- invoke_static(
    sc,
    "com.twosigma.flint.timeseries.Summarizers",
    "sum",
    column
  )

  summarize_windows(ts_rdd, window_obj, sum_summarizer)
}

#' Average summarizer
#'
#' Compute moving average of `column` and store results in a new column named
#' `<column>_mean`
#'
#' @inheritParams summarizers
#'
#' @export
summarize_avg <- function(ts_rdd, window, column) {
  sc <- spark_connection(ts_rdd)
  window_obj <- new_window_obj(sc, rlang::enexpr(window))

  avg_summarizer <- invoke_static(
    sc,
    "com.twosigma.flint.timeseries.Summarizers",
    "mean",
    column
  )

  summarize_windows(ts_rdd, window_obj, avg_summarizer)
}

#' Weighted average summarizer
#'
#' Compute moving weighted average, weighted standard deviation, weighted t-
#' stat, and observation count with the column and weight column specified and
#' store results in new columns named `<column>_<weighted_column>_mean`,
#' `<column>_<weighted_column>_weightedStandardDeviation`,
#' `<column>_<weighted_column>_weightedTStat`, and
#' `<column>_<weighted_column>_observationCount`,
#'
#' @inheritParams summarizers
#' @param weight_column Column specifying relative weight of each data point
#'
#' @export
summarize_weighted_avg <- function(ts_rdd, window, column, weight_column) {
  sc <- spark_connection(ts_rdd)
  window_obj <- new_window_obj(sc, rlang::enexpr(window))

  weighted_avg_summarizer <- invoke_static(
    sc,
    "com.twosigma.flint.timeseries.Summarizers",
    "weightedMeanTest",
    column,
    weight_column
  )

  summarize_windows(ts_rdd, window_obj, weighted_avg_summarizer)
}

#' Standard deviation summarizer
#'
#' Compute unbiased (i.e., Bessel's correction is applied) sample standard
#' deviation of values from `column` within each time window and store results
#' in a new column named `<column>_stddev`
#'
#' @inheritParams summarizers
#'
#' @export
summarize_stddev <- function(ts_rdd, window, column) {
  sc <- spark_connection(ts_rdd)
  window_obj <- new_window_obj(sc, rlang::enexpr(window))

  stddev_summarizer <- invoke_static(
    sc,
    "com.twosigma.flint.timeseries.Summarizers",
    "stddev",
    column
  )

  summarize_windows(ts_rdd, window_obj, stddev_summarizer)
}

#' Variance summarizer
#'
#' Compute variance of values from `column` within each time window and store
#' results in a new column named `<column>_variance`, with Bessel's correction
#' applied to the results
#'
#' @inheritParams summarizers
#'
#' @export
summarize_var <- function(ts_rdd, window, column) {
  sc <- spark_connection(ts_rdd)
  window_obj <- new_window_obj(sc, rlang::enexpr(window))

  var_summarizer <- invoke_static(
    sc,
    "com.twosigma.flint.timeseries.Summarizers",
    "variance",
    column
  )

  summarize_windows(ts_rdd, window_obj, var_summarizer)
}

#' Covariance summarizer
#'
#' Compute covariance between values from `xcolumn` and `ycolumn` within each time
#' window and store results in a new column named `<xcolumn>_<ycolumn>_covariance`
#'
#' @inheritParams summarizers
#' @param xcolumn Column representing the first random variable
#' @param ycolumn Column representing the second random variable
#'
#' @export
summarize_covar <- function(ts_rdd, window, xcolumn, ycolumn) {
  sc <- spark_connection(ts_rdd)
  window_obj <- new_window_obj(sc, rlang::enexpr(window))

  covar_summarizer <- invoke_static(
    sc,
    "com.twosigma.flint.timeseries.Summarizers",
    "covariance",
    xcolumn,
    ycolumn
  )

  summarize_windows(ts_rdd, window_obj, covar_summarizer)
}

#' Weighted covariance summarizer
#'
#' Compute unbiased weighted covariance between values from `xcolumn` and
#' `ycolumn` within each time window, using values from `weight_column` as
#' relative weights, and store results in a new column named
#' `"<xcolumn>_<ycolumn>_<weight_column>_weightedCovariance`
#'
#' @inheritParams summarizers
#' @param xcolumn Column representing the first random variable
#' @param ycolumn Column representing the second random variable
#' @param weight_column Column specifying relative weight of each data point
#'
#' @export
summarize_weighted_covar <- function(
  ts_rdd,
  window,
  xcolumn,
  ycolumn,
  weight_column
) {
  sc <- spark_connection(ts_rdd)
  window_obj <- new_window_obj(sc, rlang::enexpr(window))

  weighted_covar_summarizer <- invoke_static(
    sc,
    "com.twosigma.flint.timeseries.Summarizers",
    "weightedCovariance",
    xcolumn,
    ycolumn,
    weight_column
  )

  summarize_windows(ts_rdd, window_obj, weighted_covar_summarizer)
}

#' Z-score summarizer
#'
#' Computes z-score of the most recent value in the column specified, with
#' respect to the sample mean and standard deviation observed so far, with the
#' option for out-of-sample calculation, and store result in a new column named
#' `<column>_zScore`
#'
#' @inheritParams summarizers
#' @param include_current_observation If true, then use unbiased sample standard
#'   deviation with current observation in z-score calculation, otherwise use
#'   unbiased sample standard deviation excluding current observation
#'
#' @export
summarize_z_score <- function(ts_rdd, column, include_current_observation = FALSE) {
  sc <- spark_connection(ts_rdd)

  z_score_summarizer <- invoke_static(
    sc,
    "com.twosigma.flint.timeseries.Summarizers",
    "zScore",
    column,
    include_current_observation
  )

  summarize(ts_rdd, z_score_summarizer)
}

#' N-th moment summarizer
#'
#' Computes n-th moment of the column specified and store result in a new column
#' named `<column>_<n>thMoment`
#'
#' @inheritParams summarizers
#' @param n The order of moment to calculate
#'
#' @export
summarize_nth_moment <- function(ts_rdd, column, n) {
  sc <- spark_connection(ts_rdd)

  nth_moment_summarizer <- invoke_static(
    sc,
    "com.twosigma.flint.timeseries.Summarizers",
    "nthMoment",
    column,
    as.integer(n)
  )

  summarize(ts_rdd, nth_moment_summarizer)
}

#' Minimum value summarizer
#'
#' Find minimum value among values from `column` within each time window, and
#' store results in a new column named `<column>_min`
#'
#' @inheritParams summarizers
#'
#' @export
summarize_min <- function(ts_rdd, window, column) {
  sc <- spark_connection(ts_rdd)
  window_obj <- new_window_obj(sc, rlang::enexpr(window))

  min_summarizer <- invoke_static(
    sc,
    "com.twosigma.flint.timeseries.Summarizers",
    "min",
    column
  )

  summarize_windows(ts_rdd, window_obj, min_summarizer)
}

#' Maximum value summarizer
#'
#' Find maximum value among values from `column` within each time window, and
#' store results in a new column named `<column>_max`
#'
#' @inheritParams summarizers
#'
#' @export
summarize_max <- function(ts_rdd, window, column) {
  sc <- spark_connection(ts_rdd)
  window_obj <- new_window_obj(sc, rlang::enexpr(window))

  max_summarizer <- invoke_static(
    sc,
    "com.twosigma.flint.timeseries.Summarizers",
    "max",
    column
  )

  summarize_windows(ts_rdd, window_obj, max_summarizer)
}
