spark_dependencies <- function(spark_version, scala_version, ...) {
  if (spark_version < "2.0.0")
    stop("sparklyr.flint requires Spark 2.0 or higher")

  pkg_name <- "com.twosigma:sparklyr-flint_%s:%s"
  pkg_version <- "0.6.2"
  scala_version <- if (spark_version < "3.0.0") "2-11" else "2-12"

  sparklyr::spark_dependency(
    packages = sprintf(pkg_name, scala_version, pkg_version),
    repositories = "https://dl.bintray.com/yl790/maven"
  )
}

#' @import sparklyr
.onLoad <- function(libname, pkgname) {
  sparklyr::register_extension(pkgname)
}
