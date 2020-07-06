context("summarize-cycles")

sc <- testthat_spark_connection()
ts <- fromSDF(
  testthat_generic_cycles(),
  is_sorted = FALSE,
  time_unit = "SECONDS",
  time_column = "t"
)

verify_timestamps <- function(df) {
  expect_equal(as.numeric(df$time), seq(3))
}

verify_timestamps_with_id_key_column <- function(df) {
  expect_equal(as.numeric(df$time), c(1, 1, 2, 2, 3, 3))
}

test_that("summarize_count() works as expected", {
  ts_count <- summarize_count(ts) %>% collect()

  verify_timestamps(ts_count)
  expect_equal(ts_count$count, c(3, 4, 3))
})

test_that("summarize_count() with specific column works as expected", {
  ts_count <- summarize_count(ts, column = "v") %>% collect()

  verify_timestamps(ts_count)
  expect_equal(ts_count$v_count, c(2, 3, 2))
})

test_that("summarize_count() with key_columns works as expected", {
  ts_count <- summarize_count(ts, key_columns = c("id")) %>%
    collect() %>%
    dplyr::arrange(time, id)

  verify_timestamps_with_id_key_column(ts_count)
  expect_equal(ts_count$id, c(1, 2, 0, 1, 2, 3))
  expect_equal(ts_count$count, c(2, 1, 2, 2, 1, 2))
})

test_that("summarize_count() with key_columns and specific column works as expected", {
  ts_count <- summarize_count(ts, column = "v", key_columns = c("id")) %>%
    collect()

  verify_timestamps_with_id_key_column(ts_count)
  expect_equal(ts_count$id, c(1, 2, 0, 1, 2, 3))
  expect_equal(ts_count$v_count, c(1, 1, 1, 2, 1, 1))
})

test_that("summarize_min() works as expected", {
  ts_min <- summarize_min(ts, column = "v") %>% collect()

  verify_timestamps(ts_min)
  expect_equal(ts_min$v_min, c(-2, -4, 3))
})

test_that("summarize_min() with key_columns works as expected", {
  ts_min <- summarize_min(ts, column = "v", key_columns = c("id")) %>%
    collect() %>%
    dplyr::arrange(time, id)

  verify_timestamps_with_id_key_column(ts_min)
  expect_equal(ts_min$v_min, c(4, -2, 5, -4, 5, 3))
})

test_that("summarize_max() works as expected", {
  ts_max <- summarize_max(ts, column = "v") %>% collect()

  verify_timestamps(ts_max)
  expect_equal(ts_max$v_max, c(4, 5, 5))
})

test_that("summarize_max() with key_columns works as expected", {
  ts_max <- summarize_max(ts, column = "v", key_columns = c("id")) %>%
    collect() %>%
    dplyr::arrange(time, id)

  verify_timestamps_with_id_key_column(ts_max)
  expect_equal(ts_max$v_max, c(4, -2, 5, 1, 5, 3))
})

test_that("summarize_sum() works as expected", {
  ts_sum <- summarize_sum(ts, column = "v") %>% collect()

  verify_timestamps(ts_sum)
  expect_equal(ts_sum$v_sum, c(2, 2, 8))
})

test_that("summarize_sum() with key_columns works as expected", {
  ts_sum <- summarize_sum(ts, column = "v", key_columns = c("id")) %>%
    collect() %>%
    dplyr::arrange(time, id)

  verify_timestamps_with_id_key_column(ts_sum)
  expect_equal(ts_sum$v_sum, c(4, -2, 5, -3, 5, 3))
})

test_that("summarize_avg() works as expected", {
  ts_avg <- summarize_avg(ts, column = "v") %>% collect()

  verify_timestamps(ts_avg)
  expect_equal(ts_avg$v_mean, c(1, 0.66666667, 4), tolerance = 1e-7, scale = 1)
})

test_that("summarize_avg() with key_columns works as expected", {
  ts_avg <- summarize_avg(ts, column = "v", key_columns = c("id")) %>%
    collect() %>%
    dplyr::arrange(time, id)

  verify_timestamps_with_id_key_column(ts_avg)
  expect_equal(ts_avg$v_mean, c(4, -2, 5, -1.5, 5, 3))
})

test_that("summarize_weighted_avg() works as expected", {
  ts_weighted_avg <- summarize_weighted_avg(
    ts,
    column = "v",
    weight_column = "w"
  ) %>% collect()

  verify_timestamps(ts_weighted_avg)
  expect_equal(ts_weighted_avg$v_w_weightedMean, c(2, 0.6, 3.4))
  expect_equal(
    ts_weighted_avg$v_w_weightedStandardDeviation,
    c(4.2426407, 5.0373604, 1.4142136),
    tolerance = 1e-7,
    scale = 1
  )
})

test_that("summarize_weighted_avg() with key_columns works as expected", {
  ts_weighted_avg <- summarize_weighted_avg(
    ts,
    column = "v",
    weight_column = "w",
    key_columns = c("id")
  ) %>%
    collect() %>%
    dplyr::arrange(time, id)

  verify_timestamps_with_id_key_column(ts_weighted_avg)
  expect_equal(
    ts_weighted_avg$v_w_weightedMean,
    c(4, -2, 5, -2.33333333, 5,  3),
    tolerance = 1e-7,
    scale = 1
  )
  expect_equal(
    ts_weighted_avg$v_w_weightedStandardDeviation,
    c(NaN, NaN, NaN, 3.53553391, NaN, NaN),
    tolerance = 1e-7,
    scale = 1
  )
})
