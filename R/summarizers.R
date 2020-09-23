#' @include sdf_utils.R
#' @include window_exprs.R
NULL

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
#'    on records grouped by timestamps
#' @param column Column to be summarized
#' @param key_columns Optional list of columns that will form an equivalence
#'   relation associating each record with the time series it belongs to (i.e.,
#'   any 2 records having equal values in those columns will be associated with
#'   the same time series, and any 2 records having differing values in those
#'   columns are considered to be from 2 separate time series and will therefore
#'   be summarized separately)
#'   By default, `key_colums` is empty and all records are considered to be part
#'   of a single time series.
#' @param incremental If FALSE and `key_columns` is empty, then apply the
#'   summarizer to all records of `ts_rdd`.
#'   If FALSE and `key_columns` is non-empty, then apply the summarizer to all
#'   records within each group determined by `key_columns`.
#'   If TRUE and `key_columns` is empty, then for each record in `ts_rdd`,
#'   the summarizer is applied to that record and all records preceding it, and
#'   the summarized result is associated with the timestamp of that record.
#'   If TRUE and `key_columns` is non-empty, then for each record within a group
#'   of records determined by 1 or more key columns, the summarizer is applied
#'   to that record and all records preceding it within its group, and the
#'   summarized result is associated with the timestamp of that record.
#'
#' @name summarizers
NULL

new_window_obj <- function(sc, window_expr) {
  if (is.null(window_expr)) {
    NULL
  } else {
    window_expr$sc <- sc
    rlang::eval_tidy(window_expr)
  }
}

new_summarizer <- function(sc, summarizer_args) {
  summarizer_args <- append(
    list(
      sc = sc,
      class = "com.twosigma.flint.timeseries.Summarizers"
    ),
    summarizer_args
  )

  do.call(invoke_static, summarizer_args)
}

summarize_time_range <- function(
                                 ts_rdd,
                                 window_expr,
                                 summarizer_args,
                                 key_columns) {
  sc <- spark_connection(ts_rdd)
  window_obj <- new_window_obj(sc, window_expr)
  summarizer <- new_summarizer(sc, summarizer_args)

  key_columns <- as.list(key_columns)
  new_ts_rdd(
    if (is.null(window_obj)) {
      ts_rdd %>%
        spark_jobj() %>%
        invoke("summarizeCycles", summarizer, key_columns)
    } else {
      ts_rdd %>%
        spark_jobj() %>%
        invoke("summarizeWindows", window_obj, summarizer, key_columns)
    }
  )
}

summarize <- function(
                      ts_rdd,
                      summarizer_args,
                      key_columns = list(),
                      incremental = FALSE) {
  sc <- spark_connection(ts_rdd)
  summarizer <- new_summarizer(sc, summarizer_args)
  method <- (
    if (incremental) {
      "addSummaryColumns"
    } else {
      "summarize"
    }
  )

  ts_rdd %>%
    spark_jobj() %>%
    invoke(method, summarizer, as.list(key_columns)) %>%
    new_ts_rdd()
}

#' Count summarizer
#'
#' Count the total number of records if no column is specified, or the number of
#' non-null values within the specified column within each time window or within
#' each group of records with identical timestamps
#'
#' @inheritParams summarizers
#' @param column If not NULL, then report the number of values in the column
#'   specified that are not NULL or NaN within each time window or group of records
#'   with identical timestamps, and store the counts in a new column named
#'   `<column>_count`.
#'   Otherwise the number of records within each time window or group of records with
#'   identical timestamps is reported, and stored in a column named `count`.
#'
#' @return A TimeSeriesRDD containing the summarized result
#'
#' @family summarizers
#'
#' @examples
#'
#' library(sparklyr)
#' library(sparklyr.flint)
#'
#' sc <- try_spark_connect(master = "local")
#'
#' if (!is.null(sc)) {
#'   sdf <- copy_to(sc, tibble::tibble(t = seq(10), v = seq(10)))
#'   ts <- fromSDF(sdf, is_sorted = TRUE, time_unit = "SECONDS", time_column = "t")
#'   ts_count <- summarize_count(ts, column = "v", window = in_past("3s"))
#' } else {
#'   message("Unable to establish a Spark connection!")
#' }
#'
#' @export
summarize_count <- function(
                            ts_rdd,
                            column = NULL,
                            window = NULL,
                            key_columns = list()) {
  summarizer_args <- list(method = "count")
  if (!is.null(column)) summarizer_args <- append(summarizer_args, column)

  summarize_time_range(ts_rdd, rlang::enexpr(window), summarizer_args, key_columns)
}

#' Minimum value summarizer
#'
#' Find minimum value among values from `column` within each time window or
#' within each group of records with identical timestamps, and store results in a
#' new column named `<column>_min`
#'
#' @inheritParams summarizers
#'
#' @return A TimeSeriesRDD containing the summarized result
#'
#' @family summarizers
#'
#' @examples
#'
#' library(sparklyr)
#' library(sparklyr.flint)
#'
#' sc <- try_spark_connect(master = "local")
#'
#' if (!is.null(sc)) {
#'   sdf <- copy_to(sc, tibble::tibble(t = seq(10), v = seq(10)))
#'   ts <- fromSDF(sdf, is_sorted = TRUE, time_unit = "SECONDS", time_column = "t")
#'   ts_min <- summarize_min(ts, column = "v", window = in_past("3s"))
#' } else {
#'   message("Unable to establish a Spark connection!")
#' }
#'
#' @export
summarize_min <- function(ts_rdd, column, window = NULL, key_columns = list()) {
  summarizer_args <- list(method = "min", column)

  summarize_time_range(ts_rdd, rlang::enexpr(window), summarizer_args, key_columns)
}

#' Maximum value summarizer
#'
#' Find maximum value among values from `column` within each time window or
#' within each group of records with identical timestamps, and store results in a
#' new column named `<column>_max`
#'
#' @inheritParams summarizers
#'
#' @return A TimeSeriesRDD containing the summarized result
#'
#' @family summarizers
#'
#' @examples
#'
#' library(sparklyr)
#' library(sparklyr.flint)
#'
#' sc <- try_spark_connect(master = "local")
#'
#' if (!is.null(sc)) {
#'   sdf <- copy_to(sc, tibble::tibble(t = seq(10), v = seq(10)))
#'   ts <- fromSDF(sdf, is_sorted = TRUE, time_unit = "SECONDS", time_column = "t")
#'   ts_max <- summarize_max(ts, column = "v", window = in_past("3s"))
#' } else {
#'   message("Unable to establish a Spark connection!")
#' }
#'
#' @export
summarize_max <- function(ts_rdd, column, window = NULL, key_columns = list()) {
  summarizer_args <- list(method = "max", column)

  summarize_time_range(ts_rdd, rlang::enexpr(window), summarizer_args, key_columns)
}

#' Sum summarizer
#'
#' Compute moving sums on the column specified and store results in a new column
#' named `<column>_sum`
#'
#' @inheritParams summarizers
#'
#' @return A TimeSeriesRDD containing the summarized result
#'
#' @family summarizers
#'
#' @examples
#'
#' library(sparklyr)
#' library(sparklyr.flint)
#'
#' sc <- try_spark_connect(master = "local")
#'
#' if (!is.null(sc)) {
#'   sdf <- copy_to(sc, tibble::tibble(t = seq(10), v = seq(10)))
#'   ts <- fromSDF(sdf, is_sorted = TRUE, time_unit = "SECONDS", time_column = "t")
#'   ts_sum <- summarize_sum(ts, column = "v", window = in_past("3s"))
#' } else {
#'   message("Unable to establish a Spark connection!")
#' }
#'
#' @export
summarize_sum <- function(ts_rdd, column, window = NULL, key_columns = list()) {
  summarizer_args <- list(method = "sum", column)

  summarize_time_range(ts_rdd, rlang::enexpr(window), summarizer_args, key_columns)
}

#' Product summarizer
#'
#' Compute product of values from the given column within a moving time window
#  or within each group of records with identical timestamps and store results in a
#' new column named `<column>_product`
#'
#' @inheritParams summarizers
#'
#' @return A TimeSeriesRDD containing the summarized result
#'
#' @family summarizers
#'
#' @examples
#'
#' library(sparklyr)
#' library(sparklyr.flint)
#'
#' sc <- try_spark_connect(master = "local")
#'
#' if (!is.null(sc)) {
#'   sdf <- copy_to(sc, tibble::tibble(t = seq(10), v = seq(10)))
#'   ts <- fromSDF(sdf, is_sorted = TRUE, time_unit = "SECONDS", time_column = "t")
#'   ts_product <- summarize_product(ts, column = "v", window = in_past("3s"))
#' } else {
#'   message("Unable to establish a Spark connection!")
#' }
#'
#' @export
summarize_product <- function(ts_rdd, column, window = NULL, key_columns = list()) {
  summarizer_args <- list(method = "product", column)

  summarize_time_range(ts_rdd, rlang::enexpr(window), summarizer_args, key_columns)
}

#' Dot product summarizer
#'
#' Compute dot product of values from `xcolumn` and `ycolumn` within a moving
#' time window or within each group of records with identical timestamps and store
#' results in a new column named `<xcolumn>_<ycolumn>_dotProduct`
#'
#' @inheritParams summarizers
#' @param xcolumn Name of the first column
#' @param ycolumn Name of the second column
#'
#' @return A TimeSeriesRDD containing the summarized result
#'
#' @family summarizers
#'
#' @examples
#'
#' library(sparklyr)
#' library(sparklyr.flint)
#'
#' sc <- try_spark_connect(master = "local")
#'
#' if (!is.null(sc)) {
#'   sdf <- copy_to(sc, tibble::tibble(t = seq(10), u = seq(10, 1, -1), v = seq(10)))
#'   ts <- fromSDF(sdf, is_sorted = TRUE, time_unit = "SECONDS", time_column = "t")
#'   ts_dot_product <- summarize_dot_product(ts, xcolumn = "u", ycolumn = "v", window = in_past("3s"))
#' } else {
#'   message("Unable to establish a Spark connection!")
#' }
#'
#' @export
summarize_dot_product <- function(ts_rdd, xcolumn, ycolumn, window = NULL, key_columns = list()) {
  summarizer_args <- list(method = "dotProduct", xcolumn, ycolumn)

  summarize_time_range(ts_rdd, rlang::enexpr(window), summarizer_args, key_columns)
}

#' Average summarizer
#'
#' Compute moving average of `column` and store results in a new column named
#' `<column>_mean`
#'
#' @inheritParams summarizers
#'
#' @return A TimeSeriesRDD containing the summarized result
#'
#' @family summarizers
#'
#' @examples
#'
#' library(sparklyr)
#' library(sparklyr.flint)
#'
#' sc <- try_spark_connect(master = "local")
#'
#' if (!is.null(sc)) {
#'   sdf <- copy_to(sc, tibble::tibble(t = seq(10), v = seq(10)))
#'   ts <- fromSDF(sdf, is_sorted = TRUE, time_unit = "SECONDS", time_column = "t")
#'   ts_avg <- summarize_avg(ts, column = "v", window = in_past("3s"))
#' } else {
#'   message("Unable to establish a Spark connection!")
#' }
#'
#' @export
summarize_avg <- function(ts_rdd, column, window = NULL, key_columns = list()) {
  summarizer_args <- list(method = "mean", column)

  summarize_time_range(ts_rdd, rlang::enexpr(window), summarizer_args, key_columns)
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
#' @return A TimeSeriesRDD containing the summarized result
#'
#' @family summarizers
#'
#' @examples
#'
#' library(sparklyr)
#' library(sparklyr.flint)
#'
#' sc <- try_spark_connect(master = "local")
#'
#' if (!is.null(sc)) {
#'   sdf <- copy_to(sc, tibble::tibble(t = seq(10), v = seq(10), w = seq(1, 0.1, -0.1)))
#'   ts <- fromSDF(sdf, is_sorted = TRUE, time_unit = "SECONDS", time_column = "t")
#'   ts_weighted_avg <- summarize_weighted_avg(
#'     ts,
#'     column = "v", weight_column = "w", window = in_past("3s")
#'   )
#' } else {
#'   message("Unable to establish a Spark connection!")
#' }
#'
#' @export
summarize_weighted_avg <- function(
                                   ts_rdd,
                                   column,
                                   weight_column,
                                   window = NULL,
                                   key_columns = list()) {
  summarizer_args <- list(method = "weightedMeanTest", column, weight_column)

  summarize_time_range(ts_rdd, rlang::enexpr(window), summarizer_args, key_columns)
}

#' Standard deviation summarizer
#'
#' Compute unbiased (i.e., Bessel's correction is applied) sample standard
#' deviation of values from `column` within each time window or within each
#' group of records with identical timestamps, and store results in a new column
#' named `<column>_stddev`
#'
#' @inheritParams summarizers
#'
#' @return A TimeSeriesRDD containing the summarized result
#'
#' @family summarizers
#'
#' @examples
#'
#' library(sparklyr)
#' library(sparklyr.flint)
#'
#' sc <- try_spark_connect(master = "local")
#'
#' if (!is.null(sc)) {
#'   sdf <- copy_to(sc, tibble::tibble(t = seq(10), v = seq(10)))
#'   ts <- fromSDF(sdf, is_sorted = TRUE, time_unit = "SECONDS", time_column = "t")
#'   ts_stddev <- summarize_stddev(ts, column = "v", window = in_past("3s"))
#' } else {
#'   message("Unable to establish a Spark connection!")
#' }
#'
#' @export
summarize_stddev <- function(ts_rdd, column, window = NULL, key_columns = list()) {
  summarizer_args <- list(method = "stddev", column)

  summarize_time_range(ts_rdd, rlang::enexpr(window), summarizer_args, key_columns)
}

#' Variance summarizer
#'
#' Compute variance of values from `column` within each time window or within
#' each group of records with identical timestamps, and store results in a new column
#' named `<column>_variance`, with Bessel's correction applied to the results
#'
#' @inheritParams summarizers
#'
#' @return A TimeSeriesRDD containing the summarized result
#'
#' @family summarizers
#'
#' @examples
#'
#' library(sparklyr)
#' library(sparklyr.flint)
#'
#' sc <- try_spark_connect(master = "local")
#'
#' if (!is.null(sc)) {
#'   sdf <- copy_to(sc, tibble::tibble(t = seq(10), v = seq(10)))
#'   ts <- fromSDF(sdf, is_sorted = TRUE, time_unit = "SECONDS", time_column = "t")
#'   ts_var <- summarize_var(ts, column = "v", window = in_past("3s"))
#' } else {
#'   message("Unable to establish a Spark connection!")
#' }
#'
#' @export
summarize_var <- function(ts_rdd, column, window = NULL, key_columns = list()) {
  summarizer_args <- list(method = "variance", column)

  summarize_time_range(ts_rdd, rlang::enexpr(window), summarizer_args, key_columns)
}

#' Covariance summarizer
#'
#' Compute covariance between values from `xcolumn` and `ycolumn` within each time
#' window or within each group of records with identical timestamps, and store results
#' in a new column named `<xcolumn>_<ycolumn>_covariance`
#'
#' @inheritParams summarizers
#' @param xcolumn Column representing the first random variable
#' @param ycolumn Column representing the second random variable
#'
#' @return A TimeSeriesRDD containing the summarized result
#'
#' @family summarizers
#'
#' @examples
#'
#' library(sparklyr)
#' library(sparklyr.flint)
#'
#' sc <- try_spark_connect(master = "local")
#'
#' if (!is.null(sc)) {
#'   sdf <- copy_to(sc, tibble::tibble(t = seq(10), u = rnorm(10), v = rnorm(10)))
#'   ts <- fromSDF(sdf, is_sorted = TRUE, time_unit = "SECONDS", time_column = "t")
#'   ts_covar <- summarize_covar(ts, xcolumn = "u", ycolumn = "v", window = in_past("3s"))
#' } else {
#'   message("Unable to establish a Spark connection!")
#' }
#'
#' @export
summarize_covar <- function(
                            ts_rdd,
                            xcolumn,
                            ycolumn,
                            window = NULL,
                            key_columns = list()) {
  summarizer_args <- list(method = "covariance", xcolumn, ycolumn)

  summarize_time_range(ts_rdd, rlang::enexpr(window), summarizer_args, key_columns)
}

#' Weighted covariance summarizer
#'
#' Compute unbiased weighted covariance between values from `xcolumn` and
#' `ycolumn` within each time window or within each group of records with identical
#' timestamps, using values from `weight_column` as relative weights, and store
#' results in a new column named
#' `<xcolumn>_<ycolumn>_<weight_column>_weightedCovariance`
#'
#' @inheritParams summarizers
#' @param xcolumn Column representing the first random variable
#' @param ycolumn Column representing the second random variable
#' @param weight_column Column specifying relative weight of each data point
#'
#' @return A TimeSeriesRDD containing the summarized result
#'
#' @family summarizers
#'
#' @examples
#'
#' library(sparklyr)
#' library(sparklyr.flint)
#'
#' sc <- try_spark_connect(master = "local")
#'
#' if (!is.null(sc)) {
#'   sdf <- copy_to(sc, tibble::tibble(t = seq(10), u = rnorm(10), v = rnorm(10), w = 1.1^seq(10)))
#'   ts <- fromSDF(sdf, is_sorted = TRUE, time_unit = "SECONDS", time_column = "t")
#'   ts_weighted_covar <- summarize_weighted_covar(
#'     ts,
#'     xcolumn = "u", ycolumn = "v", weight_column = "w", window = in_past("3s")
#'   )
#' } else {
#'   message("Unable to establish a Spark connection!")
#' }
#'
#' @export
summarize_weighted_covar <- function(
                                     ts_rdd,
                                     xcolumn,
                                     ycolumn,
                                     weight_column,
                                     window = NULL,
                                     key_columns = list()) {
  summarizer_args <- list(
    method = "weightedCovariance", xcolumn, ycolumn, weight_column
  )

  summarize_time_range(ts_rdd, rlang::enexpr(window), summarizer_args, key_columns)
}

#' Quantile summarizer
#'
#' Compute quantiles of `column` within each time window or within each group of
#' records with identical time-stamps, and store results in new columns named
#' `<column>_<quantile value>quantile`
#'
#' @inheritParams summarizers
#' @param column Column to be summarized
#' @param p List of quantile probabilities
#'
#' @return A TimeSeriesRDD containing the summarized result
#'
#' @family summarizers
#'
#' @examples
#'
#' library(sparklyr)
#' library(sparklyr.flint)
#'
#' sc <- try_spark_connect(master = "local")
#'
#' if (!is.null(sc)) {
#'   sdf <- copy_to(sc, tibble::tibble(t = seq(10), v = seq(10)))
#'   ts <- fromSDF(sdf, is_sorted = TRUE, time_unit = "SECONDS", time_column = "t")
#'   ts_quantile <- summarize_quantile(
#'     ts, column = "v", p = c(0.5, 0.75, 0.99), window = in_past("3s")
#'   )
#' } else {
#'   message("Unable to establish a Spark connection!")
#' }
#'
#' @export
summarize_quantile <- function(
                               ts_rdd,
                               column,
                               p,
                               window = NULL,
                               key_columns = list()) {
  summarizer_args <- list(method = "quantile", column, as.list(p))

  summarize_time_range(ts_rdd, rlang::enexpr(window), summarizer_args, key_columns)
}

#' Z-score summarizer
#'
#' Compute z-score of value(s) in the column specified, with respect to the
#' sample mean and standard deviation observed so far, with the option for out-
#' of-sample calculation, and store result in a new column named
#' `<column>_zScore`.
#'
#' @inheritParams summarizers
#' @param include_current_observation If true, then use unbiased sample standard
#'   deviation with current observation in z-score calculation, otherwise use
#'   unbiased sample standard deviation excluding current observation
#'
#' @return A TimeSeriesRDD containing the summarized result
#'
#' @family summarizers
#'
#' @examples
#'
#' library(sparklyr)
#' library(sparklyr.flint)
#'
#' sc <- try_spark_connect(master = "local")
#'
#' if (!is.null(sc)) {
#'   sdf <- copy_to(sc, tibble::tibble(t = seq(10), v = rnorm(10)))
#'   ts <- fromSDF(sdf, is_sorted = TRUE, time_unit = "SECONDS", time_column = "t")
#'   ts_z_score <- summarize_z_score(ts, column = "v", include_current_observation = TRUE)
#' } else {
#'   message("Unable to establish a Spark connection!")
#' }
#'
#' @export
summarize_z_score <- function(
                              ts_rdd,
                              column,
                              include_current_observation = FALSE,
                              key_columns = list(),
                              incremental = FALSE) {
  summarizer_args <- list(method = "zScore", column, include_current_observation)

  summarize(ts_rdd, summarizer_args, key_columns, incremental)
}

#' N-th moment summarizer
#'
#' Compute n-th moment of the column specified and store result in a new column
#' named `<column>_<n>thMoment`
#'
#' @inheritParams summarizers
#' @param n The order of moment to calculate
#'
#' @return A TimeSeriesRDD containing the summarized result
#'
#' @family summarizers
#'
#' @examples
#'
#' library(sparklyr)
#' library(sparklyr.flint)
#'
#' sc <- try_spark_connect(master = "local")
#'
#' if (!is.null(sc)) {
#'   sdf <- copy_to(sc, tibble::tibble(t = seq(10), v = rnorm(10)))
#'   ts <- fromSDF(sdf, is_sorted = TRUE, time_unit = "SECONDS", time_column = "t")
#'   ts_4th_moment <- summarize_nth_moment(ts, column = "v", n = 4L)
#' } else {
#'   message("Unable to establish a Spark connection!")
#' }
#'
#' @export
summarize_nth_moment <- function(ts_rdd, column, n, key_columns = list(), incremental = FALSE) {
  summarizer_args <- list(method = "nthMoment", column, as.integer(n))

  summarize(ts_rdd, summarizer_args, key_columns, incremental)
}

#' N-th central moment summarizer
#'
#' Compute n-th central moment of the column specified and store result in a
#' new column named `<column>_<n>thCentralMoment`
#'
#' @inheritParams summarizers
#' @param n The order of moment to calculate
#'
#' @return A TimeSeriesRDD containing the summarized result
#'
#' @family summarizers
#'
#' @examples
#'
#' library(sparklyr)
#' library(sparklyr.flint)
#'
#' sc <- try_spark_connect(master = "local")
#'
#' if (!is.null(sc)) {
#'   sdf <- copy_to(sc, tibble::tibble(t = seq(10), v = rnorm(10)))
#'   ts <- fromSDF(sdf, is_sorted = TRUE, time_unit = "SECONDS", time_column = "t")
#'   ts_4th_central_moment <- summarize_nth_central_moment(ts, column = "v", n = 4L)
#' } else {
#'   message("Unable to establish a Spark connection!")
#' }
#'
#' @export
summarize_nth_central_moment <- function(
                                         ts_rdd,
                                         column,
                                         n,
                                         key_columns = list(),
                                         incremental = FALSE) {
  summarizer_args <- list(method = "nthCentralMoment", column, as.integer(n))

  summarize(ts_rdd, summarizer_args, key_columns, incremental)
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
#' @return A TimeSeriesRDD containing the summarized result
#'
#' @family summarizers
#'
#' @examples
#'
#' library(sparklyr)
#' library(sparklyr.flint)
#'
#' sc <- try_spark_connect(master = "local")
#'
#' if (!is.null(sc)) {
#'   sdf <- copy_to(sc, tibble::tibble(t = seq(10), u = rnorm(10), v = rnorm(10)))
#'   ts <- fromSDF(sdf, is_sorted = TRUE, time_unit = "SECONDS", time_column = "t")
#'   ts_corr <- summarize_corr(ts, columns = c("u", "v"))
#' } else {
#'   message("Unable to establish a Spark connection!")
#' }
#'
#' @export
summarize_corr <- function(ts_rdd, columns, key_columns = list(), incremental = FALSE) {
  summarizer_args <- list(method = "correlation", as.list(columns))

  summarize(ts_rdd, summarizer_args, key_columns, incremental)
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
#' @return A TimeSeriesRDD containing the summarized result
#'
#' @family summarizers
#'
#' @examples
#'
#' library(sparklyr)
#' library(sparklyr.flint)
#'
#' sc <- try_spark_connect(master = "local")
#'
#' if (!is.null(sc)) {
#'   sdf <- copy_to(
#'     sc,
#'     tibble::tibble(t = seq(10), x1 = rnorm(10), x2 = rnorm(10), y1 = rnorm(10), y2 = rnorm(10))
#'   )
#'   ts <- fromSDF(sdf, is_sorted = TRUE, time_unit = "SECONDS", time_column = "t")
#'   ts_corr2 <- summarize_corr2(ts, xcolumns = c("x1", "x2"), ycolumns = c("y1", "y2"))
#' } else {
#'   message("Unable to establish a Spark connection!")
#' }
#'
#' @export
summarize_corr2 <- function(ts_rdd, xcolumns, ycolumns, key_columns = list(), incremental = FALSE) {
  summarizer_args <- list(method = "correlation", as.list(xcolumns), as.list(ycolumns))

  summarize(ts_rdd, summarizer_args, key_columns, incremental)
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
#' @return A TimeSeriesRDD containing the summarized result
#'
#' @family summarizers
#'
#' @examples
#'
#' library(sparklyr)
#' library(sparklyr.flint)
#'
#' sc <- try_spark_connect(master = "local")
#'
#' if (!is.null(sc)) {
#'   sdf <- copy_to(sc, tibble::tibble(t = seq(10), x = rnorm(10), y = rnorm(10), w = 1.1^seq(10)))
#'   ts <- fromSDF(sdf, is_sorted = TRUE, time_unit = "SECONDS", time_column = "t")
#'   ts_weighted_corr <- summarize_weighted_corr(ts, xcolumn = "x", ycolumn = "y", weight_column = "w")
#' } else {
#'   message("Unable to establish a Spark connection!")
#' }
#'
#' @export
summarize_weighted_corr <- function(
                                    ts_rdd,
                                    xcolumn,
                                    ycolumn,
                                    weight_column,
                                    key_columns = list(),
                                    incremental = FALSE) {
  summarizer_args <- list(
    method = "weightedCorrelation",
    xcolumn,
    ycolumn,
    weight_column
  )

  summarize(ts_rdd, summarizer_args, key_columns, incremental)
}

#' Exponential weighted moving average summarizer
#'
#' Compute exponential weighted moving average (EWMA) of `column` and store
#' results in a new column named `<column>_ewma`
#' At time t[n], the i-th value x[i] with timestamp t[i] will have a weighted
#' value of [weight(i, n) * x[i]], where weight(i, n) is determined by both
#' `alpha` and `smoothing_duration`.
#'
#' @inheritParams summarizers
#' @param alpha A smoothing factor between 0 and 1 (default: 0.05) -- a higher
#'   alpha discounts older observations faster
#' @param smoothing_duration A time duration specified in string form (e.g.,
#'   "1d", "1h", "15m", etc) or "constant".
#'   The weight applied to a past observation from time t[p] at time t[n] is
#'   jointly determined by `alpha` and `smoothing_duration`.
#'
#'   If `smoothing_duration` is a fixed time duration such as "1d", then
#'   weight(p, n) = (1 - alpha) ^ [(t[n] - t[p]) / smoothing_duration]
#'
#'   If `smoothing_duration` is "constant", then
#'   weight(p, n) = (1 - alpha) ^ (n - p)
#'   (i.e., this option assumes the difference between consecutive timestamps
#'   is equal to some constant `diff`, and `smoothing_duration` is effectively
#'   also equal to `diff`, so that t[n] - t[p] = (n - p) * diff and
#'   weight(p, n) = (1 - alpha) ^ [(t[n] - t[p]) / smoothing_duration] =
#'   (1 - alpha) ^ [(n - p) * diff / diff] = (1 - alpha) ^ (n - p))
#' @param time_column Name of the column containing timestamps (default: "time")
#' @param convention One of "core" or "legacy" (default: "core")
#'
#'   If `convention` is "core", then the output will be weighted sum of all
#'   observations divided by the sum of all weight coefficients (see
#'   https://github.com/twosigma/flint/blob/master/doc/ema.md#core).
#'
#'   If `convention` is "legacy", then the output will simply be the weighted
#'   sum of all observations, without being normalized by the sum of all weight
#'   coefficients (see
#'   https://github.com/twosigma/flint/blob/master/doc/ema.md#legacy).
#'
#' @family summarizers
#'
#' @examples
#'
#' library(sparklyr)
#' library(sparklyr.flint)
#'
#' sc <- try_spark_connect(master = "local")
#'
#' if (!is.null(sc)) {
#'   price_sdf <- copy_to(
#'     sc,
#'     data.frame(
#'       time = ceiling(seq(12) / 2),
#'       price = seq(12) / 2,
#'       id = rep(c(3L, 7L), 6)
#'     )
#'   )
#'   ts <- fromSDF(price_sdf, is_sorted = TRUE, time_unit = "DAYS")
#'   ts_ewma <- summarize_ewma(
#'     ts,
#'     column = "price",
#'     smoothing_duration = "1d",
#'     key_columns = "id"
#'   )
#' } else {
#'   message("Unable to establish a Spark connection!")
#' }
#'
#' @export
summarize_ewma <- function(
                           ts_rdd,
                           column,
                           alpha = 0.05,
                           smoothing_duration = "1d",
                           time_column = "time",
                           convention = c("core", "legacy"),
                           key_columns = list()) {
  convention <- match.arg(convention)
  summarizer_args <- list(
    method = "ewma",
    column,
    alpha,
    time_column,
    smoothing_duration,
    convention
  )

  summarize(ts_rdd, summarizer_args, key_columns, incremental = TRUE)
}

#' Skewness summarizer
#'
#' Compute skewness (third standardized moment) of `column` and store the result
#' in a new column named `<column>_skewness`
#'
#' @inheritParams summarizers
#' @family summarizers
#'
#' @examples
#'
#' library(sparklyr)
#' library(sparklyr.flint)
#'
#' sc <- try_spark_connect(master = "local")
#'
#' if (!is.null(sc)) {
#'   price_sdf <- copy_to(
#'     sc,
#'     data.frame(
#'       time = ceiling(seq(12) / 2),
#'       price = seq(12) / 2,
#'       id = rep(c(3L, 7L), 6)
#'     )
#'   )
#'   ts <- fromSDF(price_sdf, is_sorted = TRUE, time_unit = "DAYS")
#'   ts_skewness <- summarize_skewness(ts, column = "price")
#' } else {
#'   message("Unable to establish a Spark connection!")
#' }
#'
#' @export
summarize_skewness <- function(
                               ts_rdd,
                               column,
                               key_columns = list(),
                               incremental = FALSE) {
  summarizer_args <- list(method = "skewness", column)

  summarize(ts_rdd, summarizer_args, key_columns, incremental)
}

#' Kurtosis summarizer
#'
#' Compute the excess kurtosis (fourth standardized moment minus 3) of `column`
#' and store the result in a new column named `<column>_kurtosis`
#'
#' @inheritParams summarizers
#' @family summarizers
#'
#' @examples
#'
#' library(sparklyr)
#' library(sparklyr.flint)
#'
#' sc <- try_spark_connect(master = "local")
#'
#' if (!is.null(sc)) {
#'   price_sdf <- copy_to(
#'     sc,
#'     data.frame(
#'       time = ceiling(seq(12) / 2),
#'       price = seq(12) / 2,
#'       id = rep(c(3L, 7L), 6)
#'     )
#'   )
#'   ts <- fromSDF(price_sdf, is_sorted = TRUE, time_unit = "DAYS")
#'   ts_kurtosis <- summarize_kurtosis(ts, column = "price")
#' } else {
#'   message("Unable to establish a Spark connection!")
#' }
#'
#' @export
summarize_kurtosis <- function(
                               ts_rdd,
                               column,
                               key_columns = list(),
                               incremental = FALSE) {
  summarizer_args <- list(method = "kurtosis", column)

  summarize(ts_rdd, summarizer_args, key_columns, incremental)
}

#' Geometric mean summarizer
#'
#' Compute geometric mean of values from `column` within a moving time window or
#' within each group of records with identical timestamps and store results in a
#' new column named `<column>_geometricMean`
#'
#' @inheritParams summarizers
#'
#' @return A TimeSeriesRDD containing the summarized result
#'
#' @family summarizers
#'
#' @examples
#'
#' library(sparklyr)
#' library(sparklyr.flint)
#'
#' sc <- try_spark_connect(master = "local")
#'
#' if (!is.null(sc)) {
#'   sdf <- copy_to(sc, tibble::tibble(t = seq(10), u = seq(10, 1, -1)))
#'   ts <- fromSDF(sdf, is_sorted = TRUE, time_unit = "SECONDS", time_column = "t")
#'   ts_geometric_mean <- summarize_geometric_mean(
#'     ts, column = "u", window = in_past("3s")
#'   )
#' } else {
#'   message("Unable to establish a Spark connection!")
#' }
#'
#' @export
summarize_geometric_mean <- function(
                                     ts_rdd,
                                     column,
                                     key_columns = list(),
                                     incremental = FALSE) {
  summarizer_args <- list(method = "geometricMean", column)

  summarize(ts_rdd, summarizer_args, key_columns, incremental)
}
