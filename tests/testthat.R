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

tbl_name <- function(name) gsub("\\.", "_", name)

testthat_sdf <- function(df_provider, sdf_key) {
  sc <- testthat_spark_connection()

  if (!exists(sdf_key, envir = .GlobalEnv)) {
    df <- df_provider()
    sdf <- sdf_copy_to(sc, df, name = tbl_name(sdf_key), overwrite = TRUE)
    assign(sdf_key, sdf, envir = .GlobalEnv)
  }

  get(sdf_key, envir = .GlobalEnv)
}

testthat_sorted_sdf <- function() {
  testthat_sdf(
    function() {
      tibble::tibble(
        time = seq(6),
        value = c(1, 4, 2, 8, 5, 7)
      )
    },
    ".testthat_sorted_sdf"
  )
}

testthat_unsorted_sdf <- function() {
  testthat_sdf(
    function() {
      tibble::tibble(
        time = c(5, 3, 6, 1, 4, 2),
        value = c(5, 2, 7, 1, 8, 4)
      )
    },
    ".testthat_unsorted_sdf"
  )
}

testthat_date_sdf <- function() {
  testthat_sdf(
    function() {
      tibble::tibble(
        date = as.Date(seq(6), origin = "1970-01-01"),
        value = c(1, 4, 2, 8, 5, 7)
      )
    },
    ".testthat_date_sdf"
  )
}

if (identical(Sys.getenv("NOT_CRAN"), "true")) {
  on.exit({ spark_disconnect_all() })

  filter <- Sys.getenv("TESTTHAT_FILTER", unset = "")
  if (identical(filter, "")) filter <- NULL

  test_check("sparklyr.flint", filter = filter)
}
