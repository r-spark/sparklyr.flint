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
  column <- rlang::enexpr(column)
  if (!is.null(column))
    args <- append(
      args,
      as.character(column)
    )

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
    as.character(rlang::enexpr(column))
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
    as.character(rlang::enexpr(column))
  )

  summarize_windows(ts_rdd, window_obj, avg_summarizer)
}

#' Weighted average summarizer
#'
#' Compute moving weighted average, weighted standard deviation, weighted t-
#' stat, and observation count with the column and weight column specified and
#' store results in new columns named `<value_column>_<weighted_column>_mean`,
#' `<value_column>_<weighted_column>_weightedStandardDeviation`,
#' `<value_column>_<weighted_column>_weightedTStat`, and
#' `<value_column>_<weighted_column>_observationCount`,
#'
#' @inheritParams summarizers
#' @param value_column Column containing data point values
#' @param weight_column Column containing relative weights of data points
#'
#' @export
summarize_weighted_avg <- function(ts_rdd, window, value_column, weight_column) {
  sc <- spark_connection(ts_rdd)
  window_obj <- new_window_obj(sc, rlang::enexpr(window))

  weighted_avg_summarizer <- invoke_static(
    sc,
    "com.twosigma.flint.timeseries.Summarizers",
    "weightedMeanTest",
    as.character(rlang::enexpr(value_column)),
    as.character(rlang::enexpr(weight_column))
  )

  summarize_windows(ts_rdd, window_obj, weighted_avg_summarizer)
}

#' Standard deviation summarizer
#'
#' Compute standard deviation of values from `column` within each time window and
#' store results in a new column named `<column>_stddev`
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
    as.character(rlang::enexpr(column))
  )

  summarize_windows(ts_rdd, window_obj, stddev_summarizer)
}

#' Variance summarizer
#'
#' Compute variance of values from `column` within each time window and store
#' results in a new column named `<column>_variance`
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
    as.character(rlang::enexpr(column))
  )

  summarize_windows(ts_rdd, window_obj, var_summarizer)
}

#' Covariance summarizer
#'
#' Compute covariance between values from `xcolumn` and `ycolumn` within each time
#' window and store results in a new column named `<xcolumn>_<ycolumn>_covariance`
#'
#' @param xcolumn Column representing the first random variable
#' @param ycolumn Column representing the second random variable
#' @inheritParams summarizers
#'
#' @export
summarize_covar <- function(ts_rdd, window, xcolumn, ycolumn) {
  sc <- spark_connection(ts_rdd)
  window_obj <- new_window_obj(sc, rlang::enexpr(window))

  covar_summarizer <- invoke_static(
    sc,
    "com.twosigma.flint.timeseries.Summarizers",
    "covariance",
    as.character(rlang::enexpr(xcolumn)),
    as.character(rlang::enexpr(ycolumn))
  )

  summarize_windows(ts_rdd, window_obj, covar_summarizer)
}
