testthat_spark_connection <- function(conn_attempts, conn_retry_interval_s = 2) {
  conn_key <- ".testthat_spark_connection"
  if (!exists(conn_key, envir = .GlobalEnv)) {
    version <- Sys.getenv("SPARK_VERSION")
    spark_installed <- spark_installed_versions()
    if (nrow(spark_installed[spark_installed$spark == version, ]) == 0) {
      spark_install(version)
    }

    conn_attempts <- 3
    for (attempt in seq(conn_attempts)) {
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
          if (attempt < conn_attempts) {
            Sys.sleep(conn_retry_interval_s)
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

testthat_generic_time_series <- function() {
  testthat_sdf(
    function() {
      tibble::tibble(
        t = c(1, 3, 4, 6, 7, 10, 15, 16, 18, 19),
        u = c(-4, 1, 3, -3, NA, 1, -4, 8, 5, 10),
        v = c(4, -2, NA, 5, NA, 1, -4, 5, NA, 3),
        w = c(1, 0.5, 1, 1, 1, 0.5, 1, 0.5, 1, 2)
      )
    },
    ".testthat_generic_time_series"
  )
}

testthat_generic_cycles <- function() {
  testthat_sdf(
    function() {
      tibble::tibble(
        t = c(1, 1, 1, 2, 2, 2, 2, 3, 3, 3),
        id = c(1, 2, 1, 0, 0, 1, 1, 2, 3, 3),
        u = c(-4, 1, 3, -3, NA, 1, -4, 8, 5, 10),
        v = c(4, -2, NA, 5, NA, 1, -4, 5, NA, 3),
        w = c(1, 0.5, 1, 1, 1, 0.5, 1, 0.5, 1, 2)
      )
    },
    ".testthat_generic_cycles"
  )
}

testthat_simple_time_series <- function() {
  testthat_sdf(
    function() {
      tibble::tibble(
        t = seq(12),
        v = seq(0.5, 6, 0.5)
      )
    },
    ".testthat_simple_time_series"
  )
}

testthat_quantile_summarizer_test_case <- function() {
  testthat_sdf(
    function() {
      tibble::tibble(
        t = seq(20),
        v = seq(20),
        c = rep(seq(5), 4),
        id = c(rep(0, 10), rep(1, 10))
      )
    },
    ".testthat_quantile_summarizer_test_case"
  )
}

testthat_corr_test_case <- function() {
  f <- c(3, 2, 6.4, -7.9, 1.4, 6)
  p <- c(0.5, 2, 3, 4, 5, 6)
  testthat_sdf(
    function() {
      tibble::tibble(
        t = seq(6),
        f = f,
        p = p,
        sp = p,
        np = -p,
        dp = p * 2,
        z = rep(0, length(f))
      )
    },
    ".testthat_corr_test_case"
  )
}

testthat_multiple_simple_ts_test_case <- function() {
  testthat_sdf(
    function() {
      tibble::tibble(
        t = do.call(c, lapply(seq(6), function(x) rep(x, 2))),
        u = c(3, 1, 4, -2, 8, 10, 7, 1, 5, 6, 7, -1),
        v = c(6, NaN, 5, 2, NaN, 3, 3, 4, 2, NaN, 1, 6),
        w = c(2, 1, 1, 2, 1, 3, 4, 2, 1, 0, 3, 1),
        id = rep(c(0, 1), 6)
      )
    },
    ".testthat_multiple_simple_ts_test_case"
  )
}

testthat_corr_multiple_ts_test_case <- function() {
  f <- c(3, 5, -1.5, 2, -2.4, 6.4, 1.5, -7.9, 4.6, 1.4, -9.6, 6)
  p <- seq(0.5, 6, 0.5)
  testthat_sdf(
    function() {
      tibble::tibble(
        t = seq(12),
        id = c(7, 3, 3, 7, 3, 7, 3, 7, 3, 7, 3, 7),
        f = f,
        p = p,
        sp = p,
        np = -p,
        dp = p * 2,
        z = rep(0, length(f))
      )
    },
    ".testthat_corr_multiple_ts_test_case"
  )
}

testthat_weighted_corr_test_case <- function() {
  testthat_sdf(
    function() {
      tibble::tibble(
        t = 1000 + 100 * c(1, seq(8)),
        id = c(0, rep(c(0, 1), 4)),
        w = seq(9, 1, -1),
        x = seq(9),
        y = seq(9, 1, -1)
      )
    },
    ".testthat_weighted_corr_test_case"
  )
}

price_ids <- c(7L, 3L, rep(c(3L, 7L), 5))
price_ts <- from_sdf(
  copy_to(
    testthat_spark_connection(),
    data.frame(
      time = ceiling(seq(12) / 2),
      price = seq(12) / 2,
      id = price_ids
    )
  ),
  is_sorted = TRUE,
  time_unit = "DAY"
)
