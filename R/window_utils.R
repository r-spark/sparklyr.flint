#' Functions for instantiating time windows
#'
#' Convenience functions for instantiating commonly used types of time windows
#'
#' @param sc Spark connection
#' @param duration String representing length of the time window containing a
#'   number followed by a time unit (e.g., "10s" or "10sec"), where time unit
#'   must be one of the following: "d", "day", "h", "hour", "min", "minute",
#'   "s", "sec", "second", "ms", "milli", "millisecond", "${\mu}$s", "micro",
#'   "microsecond", "ns", "nano", "nanosecond"
#' @name window_utils
NULL


#' Create a sliding time window capturing past and current data
#'
#' Create a sliding time window capuring data within the closed interval of
#' [current time - duration, current time]
#'
#' @rdname window_utils
#' @export
in_past <- function(sc, duration) {
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
#' @rdname window_utils
#' @export
in_future <- function(sc, duration) {
  invoke_static(
    sc,
    "com.twosigma.flint.timeseries.Windows",
    "futureAbsoluteTime",
    duration
  )
}
