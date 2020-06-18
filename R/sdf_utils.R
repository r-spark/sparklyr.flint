#' Utility functions for importing a Spark data frame into a TimeSeriesRDD
#'
#' These functions provide an interface for specifying how a Spark data frame
#' should be imported into a TimeSeriesRDD (e.g., which column represents time,
#' whether rows are already ordered by time, and time unit being used, etc)
#'
#' @name sdf_utils
#' @include globals.R
NULL


#' Converter from time unit name to Java enum value
#'
#' Convenience function to convert from the name of a time unit (e.g.,
#' "SECONDS") to its corresponding Java TimeUnit enum value
#'
#' @export
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

#' TimeSeriesRDD builder object
#'
#' Builder object containing all required info (i.e., isSorted, timeUnit, and
#' timeColumn) for importing a Spark data frame into a TimeSeriesRDD
#'
#' @export
ts_rdd_builder <- function(
  sc,
  is_sorted = FALSE,
  time_unit = .sparklyr.flint.globals$kValidTimeUnits,
  time_column = .sparklyr.flint.globals$kDefaultTimeColumn
) {
  time_unit <- match.arg(time_unit)
  structure(list(
    .builder <- new_ts_rdd_builder(
      sc,
      is_sorted,
      time_unit,
      time_column
    ),
    fromRDD = function(rdd, schema) {
      invoke(.builder, "fromRDD", rdd, schema)
    },
    fromDF = function(sdf) {
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
        args <- list(
          dplyr::sql(paste0("CAST (", time_column_sql, " AS LONG)"))
        )
        names(args) <- time_column
        sdf <- do.call(dplyr::mutate, c(list(sdf), args))
      }

      invoke(.builder, "fromDF", spark_dataframe(sdf))
    }
  ))
}
