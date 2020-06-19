library(sparklyr)
library(sparklyr.flint)
library(testthat)

testthat_spark_connection <- function() {
  conn_key <- ".testthat_spark_connection"
  if (!exists(conn_key, envir = .GlobalEnv)) {
    version <- Sys.getenv("SPARK_VERSION")
    spark_installed <- spark_installed_versions()
    if (nrow(spark_installed[spark_installed$spark == version, ]) == 0)
      spark_install(version)

    num_attempts <- 3
    for (attempt in seq(num_attempts)) {
      success <- tryCatch(
        {
          sc <- spark_connect(
            master = "local",
            method = "shell",
            app_name = paste0("testthat-", uuid::UUIDgenerate()),
            version = version
          )
          assign(conn_key, sc, envir = .GlobalEnv)
          TRUE
        },
        error = function(e) {
          if (attempt < num_attempts) {
            Sys.sleep(2)
            FALSE
          } else {
            e
          }
        }
      )
      if (success) break
    }
  }

  get(conn_key, envir = .GlobalEnv)
}

testthat_sorted_sdf <- function() {
  sc <- testthat_spark_connection()

  sdf_key <- ".testthat_sorted_sdf"
  if (!exists(sdf_key, envir = .GlobalEnv)) {
    df <- tibble::tibble(
      time = seq(1, 6),
      value = c(1, 4, 2, 8, 5, 7)
    )
    sdf <- sdf_copy_to(sc, df)
    assign(sdf_key, sdf, envir = .GlobalEnv)
  }

  get(sdf_key, envir = .GlobalEnv)
}

if (identical(Sys.getenv("NOT_CRAN"), "true")) {
  on.exit({ spark_disconnect_all() })

  filter <- Sys.getenv("TESTTHAT_FILTER", unset = "")
  if (identical(filter, "")) filter <- NULL

  test_check("sparklyr.flint", filter = filter)
}
