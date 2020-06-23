#' Wrapper functions for commonly used summarizer functions
#'
#' R wrapper functions for commonly used Flint summarizer functionalities such as
#' sum and count.
#'
#' @param ts_rdd Timeseries RDD being summarized
#' @param window Expression specifying time window of the summarization (TODO: example)
#' @param column Column to summarize
#' @name summarizers
#'
#' @include sdf_utils.R
#' @include window_exprs.R
NULL

summarize_window_impl <- function(ts_rdd, window_obj, summarizer, group_by = list()) {
  new_ts_rdd(invoke(ts_rdd, "summarizeWindows", window_obj, summarizer, group_by))
}

#' Count summarizer
#'
#' Count the total number of rows if no column is specified, or the number of
#' non-null values within the specified column within each time window
#'
#' @param column If not NULL, then report the number of non-NULL values in the
#'   column specified within each time window, and store the counts in a new
#'   column named `<column>_count`.
#'   Otherwise the number of rows within each time window is reported, and
#'   stored in a column named `count`.
#'
#' @rdname summarizers
#' @export
summarize_count <- function(ts_rdd, window, column = NULL) {
  sc <- spark_connection(ts_rdd)

  window <- rlang::enexpr(window)
  window$sc <- sc
  window_obj <- rlang::eval_tidy(window)

  args <- list(
    sc = sc,
    class = "com.twosigma.flint.timeseries.Summarizers",
    method = "count"
  )
  if (!is.null(column)) args <- append(args, column)

  count_summarizer <- do.call(invoke_static, args)

  summarize_window_impl(ts_rdd, window_obj, count_summarizer)
}
