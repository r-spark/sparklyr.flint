#' Dependencies and initialization procedures
#'
#' Functions in this file specify all runtime dependencies of sparklyr.flint
#' and package-wide constants in `.sparklyr.flint.globals`.
#'
#' @name init
#' @include globals.R
NULL

spark_dependencies <- function(spark_version, scala_version, ...) {
  if (spark_version < "2.0.0")
    stop("sparklyr.flint requires Spark 2.0 or higher")

  pkg_name <- "com.twosigma:sparklyr-flint_%s:%s"
  pkg_version <- "0.6.2"
  scala_version <- if (spark_version < "3.0.0") "2-11" else "2-12"

  sparklyr::spark_dependency(
    packages = c(
      sprintf(pkg_name, scala_version, pkg_version),
      "org.slf4j:slf4j-log4j12:1.7.30"
    ),
    repositories = "https://dl.bintray.com/yl790/maven"
  )
}

#' @import sparklyr
.onLoad <- function(libname, pkgname) {
  sparklyr::register_extension(pkgname)
  # initialize package-wide constants
  constants <- list(
    kValidTimeUnits = c(
      "DAYS",
      "HOURS",
      "MICROSECONDS",
      "MILLISECONDS",
      "MINUTES",
      "NANOSECONDS",
      "SECONDS"),
    kDefaultTimeColumn = "time"
  )
  for (x in names(constants)) {
    .sparklyr.flint.globals[[x]] <- constants[[x]]
    lockBinding(sym = x, env = .sparklyr.flint.globals)
  }
}
