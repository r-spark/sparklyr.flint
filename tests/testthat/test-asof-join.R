context("asof-join")

sc <- testthat_spark_connection()

ts_1 <- copy_to(
  sc,
  tibble::tibble(t = seq(10), u = seq(10), k1 = rep(c(0L, 1L), 5), k2 = rep(c(1L, 0L), 5))
) %>%
  from_sdf(is_sorted = TRUE, time_unit = "SECONDS", time_column = "t")

ts_2 <- copy_to(
  sc,
  tibble::tibble(t = seq(10) + 1, v = seq(10) + 1L)
) %>%
  from_sdf(is_sorted = TRUE, time_unit = "SECONDS", time_column = "t")

ts_3 <- copy_to(
  sc,
  tibble::tibble(t = seq(10) * 2, w = seq(10))
) %>%
  from_sdf(is_sorted = TRUE, time_unit = "SECONDS", time_column = "t")

ts_2_with_key_cols <- copy_to(
  sc,
  tibble::tibble(t = seq(10) + 1, v = seq(10) + 1L, k1 = 0L, k2 = 1L)
) %>%
  from_sdf(is_sorted = TRUE, time_unit = "SECONDS", time_column = "t")

ts_3_with_key_cols <- copy_to(
  sc,
  tibble::tibble(t = seq(10) * 2, w = seq(10), k1 = 1L, k2 = 0L)
) %>%
  from_sdf(is_sorted = TRUE, time_unit = "SECONDS", time_column = "t")

test_that("asof_join() works with direction = \">=\"", {
  rs <- asof_join(ts_2, ts_1, direction = ">=") %>% collect()
  expect_equivalent(
    rs,
    tibble::tibble(
      t = as.POSIXct(seq(10) + 1, origin = "1970-01-01"),
      v = seq(10) + 1,
      u = c(seq(9) + 1, NA),
      k1 = c(rep(c(1L, 0L), 4), 1L, NA),
      k2 = c(rep(c(0L, 1L), 4), 0L, NA)
    )
  )

  rs <- asof_join(ts_1, ts_2, direction = ">=") %>% collect()
  expect_equivalent(
    rs,
    tibble::tibble(
      t = as.POSIXct(seq(10), origin = "1970-01-01"),
      u = seq(10),
      k1 = rep(c(0L, 1L), 5),
      k2 = rep(c(1L, 0L), 5),
      v = c(NA, seq(9) + 1)
    )
  )

  rs <- asof_join(ts_3, ts_1, direction = ">=") %>% collect()
  expect_equivalent(
    rs,
    tibble::tibble(
      t = as.POSIXct(seq(10) * 2, origin = "1970-01-01"),
      w = seq(10),
      u = c(seq(5) * 2, rep(NA, 5)),
      k1 = c(rep(1L, 5), rep(NA, 5)),
      k2 = c(rep(0L, 5), rep(NA, 5))
    )
  )

  rs <- asof_join(ts_3, ts_1, tol = "5s", direction = ">=") %>% collect()
  expect_equivalent(
    rs,
    tibble::tibble(
      t = as.POSIXct(seq(10) * 2, origin = "1970-01-01"),
      w = seq(10),
      u = c(seq(5) * 2, 10L, 10L, rep(NA, 3)),
      k1 = c(rep(1L, 7), rep(NA, 3)),
      k2 = c(rep(0L, 7), rep(NA, 3))
    )
  )
})

test_that("asof_join() works with direction = \"<=\"", {
  rs <- asof_join(ts_1, ts_2, direction = "<=") %>% collect()
  expect_equivalent(
    rs,
    tibble::tibble(
      t = as.POSIXct(seq(10), origin = "1970-01-01"),
      u = seq(10),
      k1 = rep(c(0L, 1L), 5),
      k2 = rep(c(1L, 0L), 5),
      v = c(NA, seq(9) + 1)
    )
  )

  rs <- asof_join(ts_2, ts_1, direction = "<=") %>% collect()
  expect_equivalent(
    rs,
    tibble::tibble(
      t = as.POSIXct(seq(10) + 1, origin = "1970-01-01"),
      v = seq(10) + 1,
      u = c(seq(9) + 1, NA),
      k1 = c(rep(c(1L, 0L), 4), 1L, NA),
      k2 = c(rep(c(0L, 1L), 4), 0L, NA)
    )
  )

  rs <- asof_join(ts_3, ts_1, direction = "<=") %>% collect()
  expect_equivalent(
    rs,
    tibble::tibble(
      t = as.POSIXct(seq(10) * 2, origin = "1970-01-01"),
      w = seq(10),
      u = c(seq(5) * 2, rep(NA, 5)),
      k1 = c(rep(1L, 5), rep(NA, 5)),
      k2 = c(rep(0L, 5), rep(NA, 5))
    )
  )

  rs <- asof_join(ts_1, ts_2, tol = "1s", direction = "<=") %>% collect()
  expect_equivalent(
    rs,
    tibble::tibble(
      t = as.POSIXct(seq(10), origin = "1970-01-01"),
      u = seq(10),
      k1 = rep(c(0L, 1L), 5),
      k2 = rep(c(1L, 0L), 5),
      v = c(2, seq(9) + 1)
    )
  )
})

test_that("asof_join() works with direction = \"<\"", {
  rs <- asof_join(ts_1, ts_2, direction = "<") %>% collect()
  expect_equivalent(
    rs,
    tibble::tibble(
      t = as.POSIXct(seq(10), origin = "1970-01-01"),
      u = seq(10),
      k1 = rep(c(0L, 1L), 5),
      k2 = rep(c(1L, 0L), 5),
      v = NA_integer_
    )
  )

  rs <- asof_join(ts_1, ts_2, tol = "500ms", direction = "<") %>% collect()
  expect_equivalent(
    rs,
    tibble::tibble(
      t = as.POSIXct(seq(10), origin = "1970-01-01"),
      u = seq(10),
      k1 = rep(c(0L, 1L), 5),
      k2 = rep(c(1L, 0L), 5),
      v = NA_integer_
    )
  )

  rs <- asof_join(ts_1, ts_2, tol = "1s", direction = "<") %>% collect()
  expect_equivalent(
    rs,
    tibble::tibble(
      t = as.POSIXct(seq(10), origin = "1970-01-01"),
      u = seq(10),
      k1 = rep(c(0L, 1L), 5),
      k2 = rep(c(1L, 0L), 5),
      v = c(seq(10) + 1)
    )
  )
})

test_that("asof_left_join() being equivalent to Flint leftJoin", {
  actual <- asof_join(ts_2, ts_1, direction = ">=") %>% collect()
  expected <- spark_jobj(ts_2) %>%
    invoke("leftJoin", spark_jobj(ts_1), tolerance = "0ns", list(), NULL, NULL) %>%
    new_ts_rdd() %>%
    collect()
  expect_equivalent(actual, expected)

  actual <- asof_left_join(ts_1, ts_2) %>% collect()
  expected <- spark_jobj(ts_1) %>%
    invoke("leftJoin", spark_jobj(ts_2), tolerance = "0ns", list(), NULL, NULL) %>%
    new_ts_rdd() %>%
    collect()
  expect_equivalent(actual, expected)
})

test_that("asof_future_left_join() being equivalent to Flint futureLeftJoin", {
  actual <- asof_future_left_join(ts_1, ts_2) %>% collect()
  expected <- spark_jobj(ts_1) %>%
    invoke(
      "futureLeftJoin",
      spark_jobj(ts_2),
      tolerance = "0ns",
      list(),
      NULL,
      NULL,
      FALSE
    ) %>%
    new_ts_rdd() %>%
    collect()
  expect_equivalent(actual, expected)

  actual <- asof_future_left_join(
    ts_1, ts_2, tol = "1s", strict_lookahead = TRUE
  ) %>%
    collect()
  expected <- spark_jobj(ts_1) %>%
    invoke(
      "futureLeftJoin",
      spark_jobj(ts_2),
      tolerance = "1s",
      list(),
      NULL,
      NULL,
      TRUE
    ) %>%
    new_ts_rdd() %>%
    collect()
  expect_equivalent(actual, expected)
})

test_that("asof_join() works with left_prefix and right_prefix", {
  rs <- asof_join(
    ts_1, ts_2, tol = "1s", direction = ">=", left_prefix = "left", right_prefix = "right"
  ) %>% collect()
  expect_equivalent(
    rs,
    tibble::tibble(
      t = as.POSIXct(seq(10), origin = "1970-01-01"),
      left_u = seq(10),
      left_k1 = rep(c(0L, 1L), 5),
      left_k2 = rep(c(1L, 0L), 5),
      right_v = c(NA, seq(9) + 1)
    )
  )

  rs <- asof_join(
    ts_1, ts_2, tol = "1s", direction = "<=", left_prefix = "left", right_prefix = "right"
  ) %>% collect()
  expect_equivalent(
    rs,
    tibble::tibble(
      t = as.POSIXct(seq(10), origin = "1970-01-01"),
      left_u = seq(10),
      left_k1 = rep(c(0L, 1L), 5),
      left_k2 = rep(c(1L, 0L), 5),
      right_v = c(2, seq(9) + 1)
    )
  )
})

test_that("asof_join() works with key columns", {
  rs <- asof_join(
    ts_2_with_key_cols, ts_1, direction = ">=", key_columns = c("k1", "k2")
  ) %>%
    collect()
  expect_equivalent(
    rs,
    tibble::tibble(
      t = as.POSIXct(seq(10) + 1, origin = "1970-01-01"),
      v = seq(10) + 1,
      k1 = 0L,
      k2 = 1L,
      u = c(NA_integer_, 3, NA, 5, NA, 7, NA, 9, NA, NA)
    )
  )

  rs <- asof_join(
    ts_1, ts_2_with_key_cols, direction = "<=", key_columns = c("k1", "k2")
  ) %>%
    collect()
  expect_equivalent(
    rs,
    tibble::tibble(
      t = as.POSIXct(seq(10), origin = "1970-01-01"),
      u = seq(10),
      k1 = rep(c(0L, 1L), 5),
      k2 = rep(c(1L, 0L), 5),
      v = c(NA_integer_, NA, 3, NA, 5, NA, 7, NA, 9, NA)
    )
  )

  rs <- asof_join(
    ts_1, ts_2_with_key_cols, direction = "<", tol = "1s", key_columns = c("k1", "k2")
  ) %>% collect()
  expect_equivalent(
    rs,
    tibble::tibble(
      t = as.POSIXct(seq(10), origin = "1970-01-01"),
      u = seq(10),
      k1 = rep(c(0L, 1L), 5),
      k2 = rep(c(1L, 0L), 5),
      v = c(2L, NA, 4L, NA, 6L, NA, 8L, NA, 10L, NA)
    )
  )
})
