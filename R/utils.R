#' @include imports.R
NULL

#' Attempt to establish a Spark connection
#'
#' Attempt to connect to Apache Spark and return a Spark connection object upon success
#'
#' @param ... Parameters for sparklyr::spark_connect
#'
#' @return a Spark connection object if attempt was successful, or NULL otherwise
#'
#' @examples
#'
#' try_spark_connect(master = "local")
#'
#' @export
try_spark_connect <- function(...) {
  if (nrow(spark_installed_versions()) == 0) {
    message("Unable to locate any Apache Spark installation!")
    NULL
  }

  tryCatch(
    spark_connect(...),
    error = function(e) {
      message("Unable to establish a Spark connection: ", e)
      NULL
    }
  )
}
