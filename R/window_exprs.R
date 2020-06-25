#' Time window specifications
#'
#' Functions for specifying commonly used types of time windows, which are
#' commonly used within the context of summarize_* functions (e.g.,
#' `summarize_count(ts_rdd, in_past("3s"))`) where the Spark connection (`sc`)
#' is implied to be the same Spark connection the timeseries RDD object has
#' and does not need to be specified explicitly
#'
#' @param duration String representing length of the time window containing a
#'   number followed by a time unit (e.g., "10s" or "10sec"), where time unit
#'   must be one of the following: "d", "day", "h", "hour", "min", "minute",
#'   "s", "sec", "second", "ms", "milli", "millisecond", "\deqn{ \mu }s",
#'   "micro", "microsecond", "ns", "nano", "nanosecond"
#' @param sc Spark connection (does not need to be specified within the context
#'   of `summarize_*` functions)
#'
#' @name window_exprs
#' @include globals.R
NULL

#' Create a sliding time window capturing past and current data
#'
#' Create a sliding time window capuring data within the closed interval of
#' [current time - duration, current time]
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
