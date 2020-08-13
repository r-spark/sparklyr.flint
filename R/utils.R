#' Attempt to establish a Spark connection
#'
#' Call `sparklyr::spark_connect` with parameters given, returning the
#' resulting Spark connection object upon success, or NULL upon failure
#'
#' importFrom sparklyr spark_connect
#' @inheritParams sparklyr::spark_connect
#' @return a Spark connection object if attempt was successful, or NULL otherwise
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
