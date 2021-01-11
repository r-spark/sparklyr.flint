#' @include globals.R
NULL

#' Dependencies and initialization procedures
#'
#' Functions in this file specify all runtime dependencies of sparklyr.flint
#' and package-wide constants in `.sparklyr.flint.globals`.
#'
#' @name init
NULL

spark_dependencies <- function(spark_version, scala_version, ...) {
  if (spark_version < "2.0.0") {
    stop("sparklyr.flint requires Spark 2.0 or higher")
  }

  pkg_name <- "org.sparklyr:sparklyr-flint_%s_%s:%s"
  pkg_version <- "0.7.0"
  pkg_spark_version <- if (spark_version < "3.0.0") "2-4" else "3-0"
  if (!is.null(scala_version)) {
    scala_version <- if (scala_version < "2.12") "2-11" else "2-12"
  } else {
    scala_version <- if (spark_version < "3.0.0") "2-11" else "2-12"
  }

  sparklyr::spark_dependency(
    jars = "https://github.com/r-spark/sparkwarc/raw/main/inst/java/sparkwarc-3.0-2.12.jar",  # for debugging something in sparklyr
    packages = c(
      "org.slf4j:slf4j-log4j12:1.7.30",
      sprintf(pkg_name, pkg_spark_version, scala_version, pkg_version)
    ),
    repositories = "https://dl.bintray.com/yl790/maven"
  )
}

.onLoad <- function(libname, pkgname) {
  sparklyr::register_extension(pkgname)
  # initialize package-wide constants
  constants <- list(
    kValidTimeUnits = c(
      "DAYS",
      "HOURS",
      "MINUTES",
      "SECONDS",
      "MILLISECONDS",
      "MICROSECONDS",
      "NANOSECONDS"
    ),
    kDefaultTimeColumn = "time"
  )
  for (x in names(constants)) {
    .sparklyr.flint.globals[[x]] <- constants[[x]]
    lockBinding(sym = x, env = .sparklyr.flint.globals)
  }
}
