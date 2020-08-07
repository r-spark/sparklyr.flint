#' Utility functions for importing a Spark data frame into a TimeSeriesRDD
#'
#' These functions provide an interface for specifying how a Spark data frame
#' should be imported into a TimeSeriesRDD (e.g., which column represents time,
#' whether rows are already ordered by time, and time unit being used, etc)
#'
#' @param sc Spark connection
#' @param is_sorted Whether the rows being imported are already sorted by time
#' @param time_unit Time unit of the time column (must be one of the following
#'   values: "NANOSECONDS", "MICROSECONDS", "MILLISECONDS", "SECONDS",
#'   "MINUTES", "HOURS", "DAYS"
#' @param time_column Name of the time column
#'
#' @name sdf_utils
#' @include globals.R
NULL

jtime_unit <- function(sc, time_unit = .sparklyr.flint.globals$kValidTimeUnits) {
  invoke_static(sc, "java.util.concurrent.TimeUnit", match.arg(time_unit))
}

new_ts_rdd_builder <- function(sc, is_sorted, time_unit, time_column) {
  invoke_new(
    sc,
    "com.twosigma.flint.timeseries.TimeSeriesRDDBuilder",
    is_sorted,
    jtime_unit(sc, time_unit),
    time_column
  )
}

new_ts_rdd <- function(jobj) {
  class(jobj) <- c("ts_rdd", class(jobj))
  jobj
}

.fromSDF <- function(builder, time_column) {
  impl <- function(sdf) {
    schema <- invoke(spark_dataframe(sdf), "schema")
    time_column_idx <- invoke(schema, "fieldIndex", time_column)
    time_column_type <- invoke(
      schema,
      "%>%",
      list("apply", time_column_idx),
      list("dataType"),
      list("typeName")
    )
    if (!time_column_type %in% c("long", "timestamp")) {
      time_column_sql <- dbplyr::translate_sql_(
        list(rlang::sym(time_column)),
        dbplyr::simulate_dbi()
      )
      dest_type <- (
        if (identical(time_column_type, "date")) "TIMESTAMP" else "LONG"
      )
      args <- list(
        dplyr::sql(paste0("CAST (", time_column_sql, " AS ", dest_type, ")"))
      )
      names(args) <- time_column
      sdf <- do.call(dplyr::mutate, c(list(sdf), args))
    }

    new_ts_rdd(invoke(builder, "fromDF", spark_dataframe(sdf)))
  }

  impl
}

.fromRDD <- function(builder, time_column) {
  from_df_impl <- .fromSDF(builder, time_column)
  impl <- function(rdd, schema) {
    sc <- spark_connection(rdd)
    session <- spark_session(sc)
    sdf <- invoke(session, "createDataFrame", rdd, schema) %>%
      sdf_register()

    from_df_impl(sdf)
  }

  impl
}

#' TimeSeriesRDD builder object
#'
#' Builder object containing all required info (i.e., isSorted, timeUnit, and
#' timeColumn) for importing a Spark data frame into a TimeSeriesRDD
#'
#' @inheritParams sdf_utils
#'
#' @export
ts_rdd_builder <- function(
                           sc,
                           is_sorted = FALSE,
                           time_unit = .sparklyr.flint.globals$kValidTimeUnits,
                           time_column = .sparklyr.flint.globals$kDefaultTimeColumn) {
  time_unit <- match.arg(time_unit)
  structure(list(
    .builder <- new_ts_rdd_builder(
      sc,
      is_sorted = is_sorted,
      time_unit = time_unit,
      time_column = time_column
    ),
    fromSDF = .fromSDF(.builder, time_column),
    fromRDD = .fromRDD(.builder, time_column)
  ))
}

#' Construct a TimeSeriesRDD from a Spark DataFrame
#'
#' Construct a TimeSeriesRDD containing time series data from a Spark DataFrame
#'
#' @inheritParams sdf_utils
#' @param sdf A Spark DataFrame object
#'
#' @examples
#' \dontrun{
#' library(sparklyr)
#' library(sparklyr.flint)
#'
#' sc <- spark_connect(master = "local")
#'
#' sdf <- copy_to(sc, tibble::tibble(t = seq(10), v = seq(10)))
#' ts <- fromSDF(sdf, is_sorted = TRUE, time_unit = "SECONDS", time_column = "t")
#' }
#'
#' @export
fromSDF <- function(
                    sdf,
                    is_sorted = FALSE,
                    time_unit = .sparklyr.flint.globals$kValidTimeUnits,
                    time_column = .sparklyr.flint.globals$kDefaultTimeColumn) {
  sc <- spark_connection(sdf)
  builder <- ts_rdd_builder(sc, is_sorted, time_unit, time_column)
  builder$fromSDF(sdf)
}

#' Construct a TimeSeriesRDD from a Spark RDD of rows
#'
#' Construct a TimeSeriesRDD containing time series data from a Spark RDD of rows
#'
#' @inheritParams sdf_utils
#' @param rdd A Spark RDD[Row] object containing time series data
#' @param schema A Spark StructType object containing schema of the time series
#'   data
#'
#' @examples
#' \dontrun{
#' library(sparklyr)
#' library(sparklyr.flint)
#'
#' sc <- spark_connect(master = "local")
#'
#' sdf <- copy_to(sc, tibble::tibble(t = seq(10), v = seq(10)))
#' rdd <- spark_dataframe(sdf) %>% invoke("rdd")
#' schema <- spark_dataframe(sdf) %>% invoke("schema")
#' ts <- fromRDD(
#'   rdd, schema,
#'   is_sorted = TRUE, time_unit = "SECONDS", time_column = "t"
#' )
#' }
#'
#' @export
fromRDD <- function(
                    rdd,
                    schema,
                    is_sorted = FALSE,
                    time_unit = .sparklyr.flint.globals$kValidTimeUnits,
                    time_column = .sparklyr.flint.globals$kDefaultTimeColumn) {
  sc <- spark_connection(rdd)
  builder <- ts_rdd_builder(sc, is_sorted, time_unit, time_column)
  builder$fromRDD(rdd, schema)
}


#' Collect data from a TimeSeriesRDD
#'
#' Collect data from a TimeSeriesRDD into a R data frame
#'
#' @param x A com.twosigma.flint.timeseries.TimeSeriesRDD object
#' @param ... Additional arguments to `sdf_collect()`
#'
#' @examples
#' \dontrun{
#' library(sparklyr)
#' library(sparklyr.flint)
#'
#' sc <- spark_connect(master = "local")
#'
#' sdf <- copy_to(sc, tibble::tibble(t = seq(10), v = seq(10)))
#' ts <- fromSDF(sdf, is_sorted = TRUE, time_unit = "SECONDS", time_column = "t")
#' df <- ts %>% collect()
#' }
#'
#' @importFrom dplyr collect
#' @export
collect.ts_rdd <- function(x, ...) {
  invoke(x, "toDF") %>%
    sdf_register() %>%
    collect()
}
