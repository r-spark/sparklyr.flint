context("asof-join")

sc <- testthat_spark_connection()
ts_1 <- copy_to(sc, tibble::tibble(t = seq(10), u = seq(10))) %>%
  from_sdf(is_sorted = TRUE, time_unit = "SECONDS", time_column = "t")
ts_2 <- copy_to(sc, tibble::tibble(t = seq(10) + 1, v = seq(10) + 1L)) %>%
  from_sdf(is_sorted = TRUE, time_unit = "SECONDS", time_column = "t")
ts_3 <- copy_to(sc, tibble::tibble(t = seq(10) * 2, w = seq(10))) %>%
  from_sdf(is_sorted = TRUE, time_unit = "SECONDS", time_column = "t")

test_that("asof_join works with direction = \">=\"", {
  rs <- asof_join(ts_2, ts_1, direction = ">=") %>% collect()

  expect_equivalent(
    rs,
    tibble::tibble(
      t = as.POSIXct(seq(10) + 1, origin = "1970-01-01"),
      v = seq(10) + 1,
      u = c(seq(9) + 1, NA)
    )
  )

  rs <- asof_join(ts_1, ts_2, direction = ">=") %>% collect()
  expect_equivalent(
    rs,
    tibble::tibble(
      t = as.POSIXct(seq(10), origin = "1970-01-01"),
      u = seq(10),
      v = c(NA, seq(9) + 1),
    )
  )

  rs <- asof_join(ts_3, ts_1, direction = ">=") %>% collect()
  expect_equivalent(
    rs,
    tibble::tibble(
      t = as.POSIXct(seq(10) * 2, origin = "1970-01-01"),
      w = seq(10),
      u = c(seq(5) * 2, rep(NA, 5))
    )
  )

  rs <- asof_join(ts_3, ts_1, tol = "5s", direction = ">=") %>% collect()
  expect_equivalent(
    rs,
    tibble::tibble(
      t = as.POSIXct(seq(10) * 2, origin = "1970-01-01"),
      w = seq(10),
      u = c(seq(5) * 2, 10L, 10L, rep(NA, 3))
    )
  )
})

test_that("asof_join works with direction = \"<=\"", {
  rs <- asof_join(ts_1, ts_2, direction = "<=") %>% collect()

  expect_equivalent(
    rs,
    tibble::tibble(
      t = as.POSIXct(seq(10), origin = "1970-01-01"),
      u = seq(10),
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
    )
  )

  rs <- asof_join(ts_3, ts_1, direction = "<=") %>% collect()
  expect_equivalent(
    rs,
    tibble::tibble(
      t = as.POSIXct(seq(10) * 2, origin = "1970-01-01"),
      w = seq(10),
      u = c(seq(5) * 2, rep(NA, 5))
    )
  )

  rs <- asof_join(ts_1, ts_2, tol = "1s", direction = "<=") %>% collect()
  expect_equivalent(
    rs,
    tibble::tibble(
      t = as.POSIXct(seq(10), origin = "1970-01-01"),
      u = seq(10),
      v = c(2, seq(9) + 1)
    )
  )
})

test_that("asof_join works with direction = \"<\"", {
  rs <- asof_join(ts_1, ts_2, direction = "<") %>% collect()
  expect_equivalent(
    rs,
    tibble::tibble(
      t = as.POSIXct(seq(10), origin = "1970-01-01"),
      u = seq(10),
      v = NA_integer_
    )
  )

  rs <- asof_join(ts_1, ts_2, tol = "500ms", direction = "<") %>% collect()
  expect_equivalent(
    rs,
    tibble::tibble(
      t = as.POSIXct(seq(10), origin = "1970-01-01"),
      u = seq(10),
      v = NA_integer_
    )
  )

  rs <- asof_join(ts_1, ts_2, tol = "1s", direction = "<") %>% collect()
  expect_equivalent(
    rs,
    tibble::tibble(
      t = as.POSIXct(seq(10), origin = "1970-01-01"),
      u = seq(10),
      v = c(seq(10) + 1)
    )
  )
})

test_that("left_join being equivalent to Flint leftJoin", {
  actual <- asof_join(ts_2, ts_1, direction = ">=") %>% collect()
  expected <- spark_jobj(ts_2) %>%
    invoke("leftJoin", spark_jobj(ts_1), tolerance = "0ns", list(), NULL, NULL) %>%
    new_ts_rdd() %>%
    collect()
  expect_equivalent(actual, expected)

  actual <- asof_join(ts_1, ts_2, direction = ">=") %>% collect()
  expected <- spark_jobj(ts_1) %>%
    invoke("leftJoin", spark_jobj(ts_2), tolerance = "0ns", list(), NULL, NULL) %>%
    new_ts_rdd() %>%
    collect()
  expect_equivalent(actual, expected)
})

test_that("future_left_join being equivalent to Flint futureLeftJoin", {
  actual <- asof_join(ts_1, ts_2, direction = "<=") %>% collect()
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

  actual <- asof_join(ts_1, ts_2, tol = "1s", direction = "<") %>% collect()
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

test_that("asof_join works with left_prefix and right_prefix", {
  rs <- asof_join(
    ts_1, ts_2, tol = "1s", direction = ">=", left_prefix = "left", right_prefix = "right"
  ) %>% collect()
  expect_equivalent(
    rs,
    tibble::tibble(
      t = as.POSIXct(seq(10), origin = "1970-01-01"),
      left_u = seq(10),
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
      right_v = c(2, seq(9) + 1)
    )
  )
})
