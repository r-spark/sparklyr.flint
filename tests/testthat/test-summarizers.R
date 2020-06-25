context("sdf-utils")

sc <- testthat_spark_connection()
ts <- fromSDF(
  testthat_generic_time_series(),
  is_sorted = FALSE,
  time_unit = "SECONDS",
  time_column = "t"
)

test_that("count summarizer works as expected", {
  ts_count <- summarize_count(ts, in_past("3s")) %>% collect()

  expect_equal(ts_count$count, c(1, 2, 3, 3, 3, 2, 1, 2, 3, 3))
})

test_that("count summarizer with specific column works as expected", {
  ts_count <- summarize_count(ts, in_past("3s"), column = "v") %>% collect()

  expect_equal(ts_count$v_count, c(1, 2, 2, 2, 1, 1, 1, 2, 2, 2))
})

test_that("sum summarizer works as expected", {
  ts_sum <- summarize_sum(ts, in_past("3s"), column = "v") %>% collect()

  expect_equal(ts_sum$v_sum, c(4, 2, 2, 3, 5, 1, -4, 1, 1, 8))
})

test_that("avg summarizer works as expected", {
  ts_avg <- summarize_avg(
    ts,
    in_past("3s"),
    column = "v"
  ) %>% collect()

  expect_equal(
    ts_avg$v_mean,
    c(4, 1, 1, 1.5, 5, 1, -4, 0.5, 0.5, 4),
    tolerance = 1e-7,
    scale = 1
  )
})

test_that("weighted-avg summarizer works as expected", {
  ts_weighted_avg <- summarize_weighted_avg(
    ts,
    in_past("3s"),
    column = "v",
    weight_column = "w"
  ) %>% collect()

  expect_equal(
    ts_weighted_avg$v_w_weightedMean,
    c(4, 2, 2, 2.66666667, 5, 1, -4, -1, -1, 3.4),
    tolerance = 1e-7,
    scale = 1
  )
  expect_equal(
    ts_weighted_avg$v_w_weightedStandardDeviation,
    c(NaN, 4.2426407, 4.2426407, 4.9497475, NaN, NaN, NaN, 6.3639610, 6.3639610, 1.4142136),
    tolerance = 1e-7,
    scale = 1
  )
  expect_equal(
    ts_weighted_avg$v_w_weightedTStat,
    c(NaN, 0.63245553, 0.63245553, 0.72280632, NaN, NaN, NaN, -0.21081851, -0.21081851, 2.91547595),
    tolerance = 1e-7,
    scale = 1
  )
  expect_equal(ts_weighted_avg$v_w_observationCount, c(1, 2, 2, 2, 1, 1, 1, 2, 2, 2))
})

test_that("stddev summarizer works as expected", {
  ts_stddev <- summarize_stddev(
    ts,
    in_past("6s"),
    column = "v"
  ) %>% collect()

  expect_equal(
    ts_stddev$v_stddev,
    c(NaN, 4.24264069, 4.24264069, 3.78593890, 3.78593890, 2.82842712, 3.53553390, 4.50924975, 6.36396103, 4.72581563),
    tolerance = 1e-7,
    scale = 1
  )
})

test_that("var summarizer works as expected", {
  ts_var <- summarize_var(
    ts,
    in_past("6s"),
    column = "v"
  ) %>% collect()

  expect_equal(
    ts_var$v_variance,
    c(NaN, 18, 18, 14.33333333, 14.33333333, 8, 12.5, 20.33333333, 40.5, 22.33333333),
    tolerance = 1e-7,
    scale = 1
  )
})
