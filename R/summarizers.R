#' Wrapper functions for commonly used summarizer functions
#'
#' R wrapper functions for commonly used Flint summarizer functionalities such as
#' sum and count.
#'
#' @param ts_rdd Timeseries RDD being summarized
#' @param window Either a R expression specifying time windows to be summarized
#'    (e.g., `in_past("1h")` to summarize data from looking behind 1 hour at
#'    each time point, `in_future("5s")` to summarize data from looking forward
#'    5 seconds at each time point), or `NULL` to compute aggregate statistics
#'    on rows grouped by timestamps
#' @param column Column to be summarized
#' @param key_columns Optional list of columns that will form an equivalence
#'   relation associating each row with the time series it belongs to (i.e., any
#'   2 rows having equal values in those columns will be associated with the
#'   same time series, and any 2 rows having differing values in those columns
#'   are considered to be from 2 separate time series and will therefore be
#'   summarized separately)
#'   By default, `key_colums` is empty and all rows are considered to be part of
#'   a single time series.
#' @name summarizers
#'
#' @include sdf_utils.R
#' @include window_exprs.R
NULL

summarize_time_range <- function(
  ts_rdd,
  window_obj,
  summarizer,
  key_columns
) {
  new_ts_rdd(
    if (is.null(window_obj))
      invoke(
        ts_rdd,
        "summarizeCycles",
        summarizer,
        as.list(key_columns)
      )
    else
      invoke(
        ts_rdd,
        "summarizeWindows",
        window_obj,
        summarizer,
        as.list(key_columns)
      )
  )
}

summarize <- function(ts_rdd, summarizer, key_columns = list()) {
  new_ts_rdd(invoke(ts_rdd, "summarize", summarizer, as.list(key_columns)))
}

new_window_obj <- function(sc, window_expr) {
  if (is.null(window_expr)) {
    NULL
  } else {
    window_expr$sc <- sc
    rlang::eval_tidy(window_expr)
  }
}

#' Count summarizer
#'
#' Count the total number of rows if no column is specified, or the number of
#' non-null values within the specified column within each time window or within
#' each group of rows with identical timestamps
#'
#' @inheritParams summarizers
#' @param column If not NULL, then report the number of values in the column
#'   specified that are not NULL or NaN within each time window or group of rows
#'   with identical timestamps, and store the counts in a new column named
#'   `<column>_count`.
#'   Otherwise the number of rows within each time window or group of rows with
#'   identical timestamps is reported, and stored in a column named `count`.
#'
#' @export
summarize_count <- function(
  ts_rdd,
  column = NULL,
  window = NULL,
  key_columns = list()
) {
  sc <- spark_connection(ts_rdd)
  window_obj <- new_window_obj(sc, rlang::enexpr(window))

  args <- list(
    sc = sc,
    class = "com.twosigma.flint.timeseries.Summarizers",
    method = "count"
  )
  if (!is.null(column)) args <- append(args, column)

  count_summarizer <- do.call(invoke_static, args)

  summarize_time_range(ts_rdd, window_obj, count_summarizer, key_columns)
}

#' Minimum value summarizer
#'
#' Find minimum value among values from `column` within each time window or
#' within each group of rows with identical timestamps, and store results in a
#' new column named `<column>_min`
#'
#' @inheritParams summarizers
#'
#' @export
summarize_min <- function(ts_rdd, column, window = NULL, key_columns = list()) {
  sc <- spark_connection(ts_rdd)
  window_obj <- new_window_obj(sc, rlang::enexpr(window))

  min_summarizer <- invoke_static(
    sc,
    "com.twosigma.flint.timeseries.Summarizers",
    "min",
    column
  )

  summarize_time_range(ts_rdd, window_obj, min_summarizer, key_columns)
}

#' Maximum value summarizer
#'
#' Find maximum value among values from `column` within each time window or
#' within each group of rows with identical timestamps, and store results in a
#' new column named `<column>_max`
#'
#' @inheritParams summarizers
#'
#' @export
summarize_max <- function(ts_rdd, column, window = NULL, key_columns = list()) {
  sc <- spark_connection(ts_rdd)
  window_obj <- new_window_obj(sc, rlang::enexpr(window))

  max_summarizer <- invoke_static(
    sc,
    "com.twosigma.flint.timeseries.Summarizers",
    "max",
    column
  )

  summarize_time_range(ts_rdd, window_obj, max_summarizer, key_columns)
}

#' Sum summarizer
#'
#' Compute moving sums on the column specified and store results in a new column
#' named `<column>_sum`
#'
#' @inheritParams summarizers
#'
#' @export
summarize_sum <- function(ts_rdd, column, window = NULL, key_columns = list()) {
  sc <- spark_connection(ts_rdd)
  window_obj <- new_window_obj(sc, rlang::enexpr(window))

  sum_summarizer <- invoke_static(
    sc,
    "com.twosigma.flint.timeseries.Summarizers",
    "sum",
    column
  )

  summarize_time_range(ts_rdd, window_obj, sum_summarizer, key_columns)
}

#' Product summarizer
#'
#' Compute product of values from the given column within a moving time window
#  or within each group of rows with identical timestamps and store results in a
#' new column named `<column>_product`
#'
#' @inheritParams summarizers
#'
#' @export
summarize_product <- function(ts_rdd, column, window = NULL, key_columns = list()) {
  sc <- spark_connection(ts_rdd)
  window_obj <- new_window_obj(sc, rlang::enexpr(window))

  sum_summarizer <- invoke_static(
    sc,
    "com.twosigma.flint.timeseries.Summarizers",
    "product",
    column
  )

  summarize_time_range(ts_rdd, window_obj, sum_summarizer, key_columns)
}

#' Average summarizer
#'
#' Compute moving average of `column` and store results in a new column named
#' `<column>_mean`
#'
#' @inheritParams summarizers
#'
#' @export
summarize_avg <- function(ts_rdd, column, window = NULL, key_columns = list()) {
  sc <- spark_connection(ts_rdd)
  window_obj <- new_window_obj(sc, rlang::enexpr(window))

  avg_summarizer <- invoke_static(
    sc,
    "com.twosigma.flint.timeseries.Summarizers",
    "mean",
    column
  )

  summarize_time_range(ts_rdd, window_obj, avg_summarizer, key_columns)
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
summarize_weighted_avg <- function(
  ts_rdd,
  column,
  weight_column,
  window = NULL,
  key_columns = list()
) {
  sc <- spark_connection(ts_rdd)
  window_obj <- new_window_obj(sc, rlang::enexpr(window))

  weighted_avg_summarizer <- invoke_static(
    sc,
    "com.twosigma.flint.timeseries.Summarizers",
    "weightedMeanTest",
    column,
    weight_column
  )

  summarize_time_range(ts_rdd, window_obj, weighted_avg_summarizer, key_columns)
}

#' Standard deviation summarizer
#'
#' Compute unbiased (i.e., Bessel's correction is applied) sample standard
#' deviation of values from `column` within each time window or within each
#' group of rows with identical timestamps, and store results in a new column
#' named `<column>_stddev`
#'
#' @inheritParams summarizers
#'
#' @export
summarize_stddev <- function(ts_rdd, column, window = NULL, key_columns = list()) {
  sc <- spark_connection(ts_rdd)
  window_obj <- new_window_obj(sc, rlang::enexpr(window))

  stddev_summarizer <- invoke_static(
    sc,
    "com.twosigma.flint.timeseries.Summarizers",
    "stddev",
    column
  )

  summarize_time_range(ts_rdd, window_obj, stddev_summarizer, key_columns)
}

#' Variance summarizer
#'
#' Compute variance of values from `column` within each time window or within
#' each group of rows with identical timestamps, and store results in a new column
#' named `<column>_variance`, with Bessel's correction applied to the results
#'
#' @inheritParams summarizers
#'
#' @export
summarize_var <- function(ts_rdd, column, window = NULL, key_columns = list()) {
  sc <- spark_connection(ts_rdd)
  window_obj <- new_window_obj(sc, rlang::enexpr(window))

  var_summarizer <- invoke_static(
    sc,
    "com.twosigma.flint.timeseries.Summarizers",
    "variance",
    column
  )

  summarize_time_range(ts_rdd, window_obj, var_summarizer, key_columns)
}

#' Covariance summarizer
#'
#' Compute covariance between values from `xcolumn` and `ycolumn` within each time
#' window or within each group of rows with identical timestamps, and store results
#' in a new column named `<xcolumn>_<ycolumn>_covariance`
#'
#' @inheritParams summarizers
#' @param xcolumn Column representing the first random variable
#' @param ycolumn Column representing the second random variable
#'
#' @export
summarize_covar <- function(
  ts_rdd,
  xcolumn,
  ycolumn,
  window = NULL,
  key_columns = list()
) {
  sc <- spark_connection(ts_rdd)
  window_obj <- new_window_obj(sc, rlang::enexpr(window))

  covar_summarizer <- invoke_static(
    sc,
    "com.twosigma.flint.timeseries.Summarizers",
    "covariance",
    xcolumn,
    ycolumn
  )

  summarize_time_range(ts_rdd, window_obj, covar_summarizer, key_columns)
}

#' Weighted covariance summarizer
#'
#' Compute unbiased weighted covariance between values from `xcolumn` and
#' `ycolumn` within each time window or within each group of rows with identical
#' timestamps, using values from `weight_column` as relative weights, and store
#' results in a new column named
#' `<xcolumn>_<ycolumn>_<weight_column>_weightedCovariance`
#'
#' @inheritParams summarizers
#' @param xcolumn Column representing the first random variable
#' @param ycolumn Column representing the second random variable
#' @param weight_column Column specifying relative weight of each data point
#'
#' @export
summarize_weighted_covar <- function(
  ts_rdd,
  xcolumn,
  ycolumn,
  weight_column,
  window = NULL,
  key_columns = list()
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

  summarize_time_range(
    ts_rdd,
    window_obj,
    weighted_covar_summarizer,
    key_columns
  )
}

#' Quantile summarizer
#'
#' Compute quantiles of `column` within each time window or within each group of
#' rows with identical time-stamps, and store results in new columns named
#' `<column>_<quantile value>quantile`
#'
#' @inheritParams summarizers
#' @param column Column to be summarized
#' @param p List of quantile probabilities
#'
#' @export
summarize_quantile <- function(
  ts_rdd,
  column,
  p,
  window = NULL,
  key_columns = list()
) {
  sc <- spark_connection(ts_rdd)
  window_obj <- new_window_obj(sc, rlang::enexpr(window))

  quantile_summarizer <- invoke_static(
    sc,
    "com.twosigma.flint.timeseries.Summarizers",
    "quantile",
    column,
    as.list(p)
  )

  summarize_time_range(
    ts_rdd,
    window_obj,
    quantile_summarizer,
    key_columns
  )
}

#' Z-score summarizer
#'
#' Compute z-score of the most recent value in the column specified, with
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
summarize_z_score <- function(
  ts_rdd,
  column,
  include_current_observation = FALSE,
  key_columns = list()) {
  sc <- spark_connection(ts_rdd)

  z_score_summarizer <- invoke_static(
    sc,
    "com.twosigma.flint.timeseries.Summarizers",
    "zScore",
    column,
    include_current_observation
  )

  summarize(ts_rdd, z_score_summarizer, key_columns)
}

#' N-th moment summarizer
#'
#' Compute n-th moment of the column specified and store result in a new column
#' named `<column>_<n>thMoment`
#'
#' @inheritParams summarizers
#' @param n The order of moment to calculate
#'
#' @export
summarize_nth_moment <- function(ts_rdd, column, n, key_columns = list()) {
  sc <- spark_connection(ts_rdd)

  nth_moment_summarizer <- invoke_static(
    sc,
    "com.twosigma.flint.timeseries.Summarizers",
    "nthMoment",
    column,
    as.integer(n)
  )

  summarize(ts_rdd, nth_moment_summarizer, key_columns)
}

#' N-th central moment summarizer
#'
#' Compute n-th central moment of the column specified and store result in a
#' new column named `<column>_<n>thCentralMoment`
#'
#' @inheritParams summarizers
#' @param n The order of moment to calculate
#'
#' @export
summarize_nth_central_moment <- function(
  ts_rdd,
  column,
  n,
  key_columns = list()
) {
  sc <- spark_connection(ts_rdd)

  nth_central_moment_summarizer <- invoke_static(
    sc,
    "com.twosigma.flint.timeseries.Summarizers",
    "nthCentralMoment",
    column,
    as.integer(n)
  )

  summarize(ts_rdd, nth_central_moment_summarizer, key_columns)
}

#' Correlation summarizer
#'
#' Compute pairwise correations among the list of columns specified and store
#' results in new columns named with the following pattern:
#' `<column1>_<column2>_correlation` and `<column1>_<column2>_correlationTStat`,
#' where column1 and column2 are names of any 2 distinct columns
#'
#' @inheritParams summarizers
#' @param columns A list of column names
#'
#' @export
summarize_corr <- function(ts_rdd, columns, key_columns = list()) {
  sc <- spark_connection(ts_rdd)

  corr_summarizer <- invoke_static(
    sc,
    "com.twosigma.flint.timeseries.Summarizers",
    "correlation",
    as.list(columns)
  )

  summarize(ts_rdd, corr_summarizer, key_columns)
}

#' Pairwise correlation summarizer
#'
#' Compute pairwise correations for all possible pairs of columns such that the
#' first column of each pair is one of `xcolumns` and the second column of each
#' pair is one of `ycolumns`, storing results in new columns named with the
#' following pattern:
#' `<column1>_<column2>_correlation` and `<column1>_<column2>_correlationTStat`
#' for each pair of columns (column1, column2)
#'
#' @inheritParams summarizers
#' @param xcolumns A list of column names
#' @param ycolumns A list of column names disjoint from xcolumns
#'
#' @export
summarize_corr2 <- function(ts_rdd, xcolumns, ycolumns, key_columns = list()) {
  sc <- spark_connection(ts_rdd)

  corr_summarizer <- invoke_static(
    sc,
    "com.twosigma.flint.timeseries.Summarizers",
    "correlation",
    as.list(xcolumns),
    as.list(ycolumns)
  )

  summarize(ts_rdd, corr_summarizer, key_columns)
}

#' Pearson weighted correlation summarizer
#'
#' Compute Pearson weighted correlation between `xcolumn` and `ycolumn` weighted
#' by `weight_column` and store result in a new columns named
#' `<xcolumn>_<ycolumn>_<weight_column>_weightedCorrelation`
#'
#' @inheritParams summarizers
#' @param xcolumn Column representing the first random variable
#' @param ycolumn Column representing the second random variable
#' @param weight_column Column specifying relative weight of each data point
#'
#' @export
summarize_weighted_corr <- function(
  ts_rdd,
  xcolumn,
  ycolumn,
  weight_column,
  key_columns = list()
) {
  sc <- spark_connection(ts_rdd)

  weighted_corr_summarizer <- invoke_static(
    sc,
    "com.twosigma.flint.timeseries.Summarizers",
    "weightedCorrelation",
    xcolumn,
    ycolumn,
    weight_column
  )

  summarize(ts_rdd, weighted_corr_summarizer, key_columns)
}

