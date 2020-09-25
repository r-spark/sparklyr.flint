#' @include globals.R
NULL

#' Time window specifications
#'
#' Functions for specifying commonly used types of time windows, which should
#' only be used within the context of summarize_* functions (e.g.,
#' `summarize_count(ts_rdd, in_past("3s"))`). When passing a time window
#' specification to some summarize_* function, the Spark connection parameter
#' (`sc`) for the time window object will be injected and will be the same Spark
#' connection the underlying timeseries RDD object is associated with, so, `sc`
#' never needs to be specified explicitly.
#'
#' @param duration String representing length of the time window containing a
#'   number followed by a time unit (e.g., "10s" or "10sec"), where time unit
#'   must be one of the following: "d", "day", "h", "hour", "min", "minute",
#'   "s", "sec", "second", "ms", "milli", "millisecond", "\deqn{ \mu }s",
#'   "micro", "microsecond", "ns", "nano", "nanosecond"
#' @param sc Spark connection (does not need to be specified within the context
#'   of `summarize_*` functions)
#'
#' @return A time window object useable by the Flint time series library
#'
#' @family Time window expressions
#'
#' @name window_exprs
NULL

#' Create a sliding time window capturing past and current data
#'
#' Create a sliding time window capuring data within the closed interval of
#' [current time - duration, current time]
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
#' @rdname window_exprs
#' @export
in_past <- function(duration, sc) {
  invoke_static(
    sc,
    "com.twosigma.flint.timeseries.Windows",
    "pastAbsoluteTime",
    duration
  )
}

#' Create a sliding time window capturing current and future data
#'
#' Create a sliding time window capuring data within the closed interval of
#' [current time, current time + duration]
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
#'   ts_count <- summarize_count(ts, column = "v", window = in_future("3s"))
#' } else {
#'   message("Unable to establish a Spark connection!")
#' }
#'
#' @rdname window_exprs
#' @export
in_future <- function(duration, sc) {
  invoke_static(
    sc,
    "com.twosigma.flint.timeseries.Windows",
    "futureAbsoluteTime",
    duration
  )
}
