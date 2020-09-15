context("summarize-windows")

sc <- testthat_spark_connection()
ts <- from_sdf(
  testthat_generic_time_series(),
  is_sorted = FALSE,
  time_unit = "SECONDS",
  time_column = "t"
)
quantile_summarizer_test_case_ts <- from_sdf(
  testthat_quantile_summarizer_test_case(),
  is_sorted = TRUE,
  time_unit = "SECONDS",
  time_column = "t"
)
multiple_simple_ts <- from_sdf(
  testthat_multiple_simple_ts_test_case(),
  is_sorted = TRUE,
  time_unit = "SECONDS",
  time_column = "t"
)

test_that("summarize_count() works as expected", {
  ts_count <- summarize_count(ts, window = in_past("3s")) %>% collect()

  expect_equal(ts_count$count, c(1, 2, 3, 3, 3, 2, 1, 2, 3, 3))
})

test_that("summarize_count() with specific column works as expected", {
  ts_count <- summarize_count(ts, column = "v", window = in_past("3s")) %>%
    collect()

  expect_equal(ts_count$v_count, c(1, 2, 2, 2, 1, 1, 1, 2, 2, 2))
})

test_that("summarize_count() with key_columns works as expected", {
  ts_count <- summarize_count(
    multiple_simple_ts,
    window = in_past("3s"), key_columns = c("id")
  ) %>%
    collect()

  expect_equal(ts_count$id, rep(c(0, 1), 6))
  expect_equal(ts_count$count, c(1, 1, 2, 2, 3, 3, 4, 4, 4, 4, 4, 4))

  ts_count <- summarize_count(
    multiple_simple_ts,
    window = in_future("3s"), key_columns = c("id")
  ) %>%
    collect()

  expect_equal(ts_count$id, rep(c(0, 1), 6))
  expect_equal(ts_count$count, c(4, 4, 4, 4, 4, 4, 3, 3, 2, 2, 1, 1))
})

test_that("summarize_count() with key_columns and specific column works as expected", {
  ts_count <- summarize_count(
    multiple_simple_ts,
    column = "v",
    window = in_past("3s"),
    key_columns = c("id")
  ) %>% collect()

  expect_equal(ts_count$id, rep(c(0, 1), 6))
  expect_equal(ts_count$v_count, c(1, 0, 2, 1, 2, 2, 3, 3, 3, 3, 3, 3))

  ts_count <- summarize_count(
    multiple_simple_ts,
    column = "v",
    window = in_future("3s"),
    key_columns = c("id")
  ) %>% collect()

  expect_equal(ts_count$id, rep(c(0, 1), 6))
  expect_equal(ts_count$v_count, c(3, 3, 3, 3, 3, 3, 3, 2, 2, 1, 1, 1))
})

test_that("summarize_min() works as expected", {
  ts_min <- summarize_min(ts, column = "v", window = in_past("3s")) %>%
    collect()

  expect_equal(ts_min$v_min, c(4, -2, -2, -2, 5, 1, -4, -4, -4, 3))
})

test_that("summarize_min() with key_columns works as expected", {
  ts_min <- summarize_min(
    multiple_simple_ts,
    column = "v",
    window = in_past("3s"),
    key_columns = c("id")
  ) %>% collect()

  expect_equal(ts_min$id, rep(c(0, 1), 6))
  expect_equal(ts_min$v_min, c(6, NaN, 5, 2, 5, 2, 3, 2, 2, 2, 1, 3))

  ts_min <- summarize_min(
    multiple_simple_ts,
    column = "v",
    window = in_future("3s"),
    key_columns = c("id")
  ) %>% collect()

  expect_equal(ts_min$id, rep(c(0, 1), 6))
  expect_equal(ts_min$v_min, c(3, 2, 2, 2, 1, 3, 1, 4, 1, 6, 1, 6))
})

test_that("summarize_max() works as expected", {
  ts_max <- summarize_max(ts, column = "v", window = in_past("3s")) %>%
    collect()

  expect_equal(ts_max$v_max, c(4, 4, 4, 5, 5, 1, -4, 5, 5, 5))
})

test_that("summarize_max() with key_columns works as expected", {
  ts_max <- summarize_max(
    multiple_simple_ts,
    column = "v",
    window = in_past("3s"),
    key_columns = c("id")
  ) %>% collect()

  expect_equal(ts_max$id, rep(c(0, 1), 6))
  expect_equal(ts_max$v_max, c(6, NaN, 6, 2, 6, 3, 6, 4, 5, 4, 3, 6))

  ts_max <- summarize_max(
    multiple_simple_ts,
    column = "v",
    window = in_future("3s"),
    key_columns = c("id")
  ) %>% collect()

  expect_equal(ts_max$id, rep(c(0, 1), 6))
  expect_equal(ts_max$v_max, c(6, 4, 5, 4, 3, 6, 3, 6, 2, 6, 1, 6))
})

test_that("summarize_sum() works as expected", {
  ts_sum <- summarize_sum(ts, column = "v", window = in_past("3s")) %>%
    collect()

  expect_equal(ts_sum$v_sum, c(4, 2, 2, 3, 5, 1, -4, 1, 1, 8))
})

test_that("summarize_sum() with key_columns works as expected", {
  ts_sum <- summarize_sum(
    multiple_simple_ts,
    column = "v",
    window = in_past("3s"),
    key_columns = c("id")
  ) %>% collect()

  expect_equal(ts_sum$id, rep(c(0, 1), 6))
  expect_equal(ts_sum$v_sum, c(6, 0, 11, 2, 11, 5, 14, 9, 10, 9, 6, 13))

  ts_sum <- summarize_sum(
    multiple_simple_ts,
    column = "v",
    window = in_future("3s"),
    key_columns = c("id")
  ) %>% collect()

  expect_equal(ts_sum$id, rep(c(0, 1), 6))
  expect_equal(ts_sum$v_sum, c(14, 9, 10, 9, 6, 13, 6, 10, 3, 6, 1, 6))
})

test_that("summarize_product() works as expected", {
  ts_product <- summarize_product(ts, column = "v", window = in_past("3s")) %>%
    collect()

  expect_equal(ts_product$v_product, c(4, -8, -8, -10, 5, 1, -4, -20, -20, 15))
})

test_that("summarize_product() with key_columns works as expected", {
  ts_product <- summarize_product(
    multiple_simple_ts,
    column = "v",
    window = in_past("3s"),
    key_columns = c("id")
  ) %>% collect()

  expect_equal(ts_product$id, rep(c(0, 1), 6))
  expect_equal(ts_product$v_product, c(6, NaN, 30, 2, 30, 6, 90, 24, 30, 24, 6, 72))

  ts_product <- summarize_product(
    multiple_simple_ts,
    column = "v",
    window = in_future("3s"),
    key_columns = c("id")
  ) %>% collect()

  expect_equal(ts_product$id, rep(c(0, 1), 6))
  expect_equal(ts_product$v_product, c(90, 24, 30, 24, 6, 72, 6, 24, 2, 6, 1, 6))
})

test_that("summarize_dot_product() works as expected", {
  ts_dot_product <- summarize_dot_product(
    ts,
    xcolumn = "u",
    ycolumn = "v",
    window = in_past("3s")
  ) %>%
    collect()

  expect_equal(ts_dot_product$u_v_dotProduct, c(-16, -18, -18, -17, -15, 1, 16, 56, 56, 70))
})

test_that("summarize_dot_product() with key_columns works as expected", {
  ts_dot_product <- summarize_dot_product(
    multiple_simple_ts,
    xcolumn = "u",
    ycolumn = "v",
    window = in_past("3s"),
    key_columns = c("id")
  ) %>% collect()

  expect_equal(ts_dot_product$id, rep(c(0, 1), 6))
  expect_equal(ts_dot_product$u_v_dotProduct, c(18, 0, 38, -4, 38, 26, 59, 30, 51, 30, 38, 28))

  ts_dot_product <- summarize_dot_product(
    multiple_simple_ts,
    xcolumn = "u",
    ycolumn = "v",
    window = in_future("3s"),
    key_columns = c("id")
  ) %>% collect()

  expect_equal(ts_dot_product$id, rep(c(0, 1), 6))
  expect_equal(ts_dot_product$u_v_dotProduct, c(59, 30, 51, 30, 38, 28, 38, -2, 17, -6, 7, -6))
})

test_that("summarize_avg() works as expected", {
  ts_avg <- summarize_avg(
    ts,
    column = "v",
    window = in_past("3s")
  ) %>% collect()

  expect_equal(
    ts_avg$v_mean,
    c(4, 1, 1, 1.5, 5, 1, -4, 0.5, 0.5, 4),
    tolerance = 1e-7,
    scale = 1
  )
})

test_that("summarize_avg() with key_columns works as expected", {
  ts_avg <- summarize_avg(
    multiple_simple_ts,
    column = "v",
    window = in_past("3s"),
    key_columns = c("id")
  ) %>% collect()

  expect_equal(ts_avg$id, rep(c(0, 1), 6))
  expect_equal(
    ts_avg$v_mean,
    c(6, 0, 5.5, 2, 5.5, 2.5, 4.66666667, 3, 3.33333333, 3, 2, 4.33333333),
    tolerance = 1e-7,
    scale = 1
  )

  ts_avg <- summarize_avg(
    multiple_simple_ts,
    column = "v",
    window = in_future("3s"),
    key_columns = c("id")
  ) %>% collect()

  expect_equal(ts_avg$id, rep(c(0, 1), 6))
  expect_equal(
    ts_avg$v_mean,
    c(4.66666667, 3, 3.33333333, 3, 2, 4.33333333, 2, 5, 1.5, 6, 1, 6),
    tolerance = 1e-7,
    scale = 1
  )
})

test_that("summarize_weighted_avg() works as expected", {
  ts_weighted_avg <- summarize_weighted_avg(
    ts,
    column = "v",
    weight_column = "w",
    window = in_past("3s")
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

test_that("summarize_weighted_avg() with key_columns works as expected", {
  ts_weighted_avg <- summarize_weighted_avg(
    multiple_simple_ts,
    column = "v",
    weight_column = "w",
    window = in_past("3s"),
    key_columns = c("id")
  ) %>% collect()

  expect_equal(ts_weighted_avg$id, rep(c(0, 1), 6))
  expect_equal(
    ts_weighted_avg$v_w_weightedMean,
    c(6, NaN, 5.66666667, 2, 5.66666667, 2.6, 4.14285714, 3, 3.16666667, 3, 2.125, 3.83333333),
    tolerance = 1e-7,
    scale = 1
  )

  ts_weighted_avg <- summarize_weighted_avg(
    multiple_simple_ts,
    column = "v",
    weight_column = "w",
    window = in_future("3s"),
    key_columns = c("id")
  ) %>% collect()

  expect_equal(ts_weighted_avg$id, rep(c(0, 1), 6))
  expect_equal(
    ts_weighted_avg$v_w_weightedMean,
    c(4.14285714, 3, 3.16666667, 3, 2.125, 3.83333333, 2.125, 4.66666667, 1.25, 6, 1, 6),
    tolerance = 1e-7,
    scale = 1
  )
})

test_that("summarize_stddev() works as expected", {
  ts_stddev <- summarize_stddev(
    ts,
    column = "v",
    window = in_past("6s")
  ) %>% collect()

  expect_equal(
    ts_stddev$v_stddev,
    c(NaN, 4.24264069, 4.24264069, 3.78593890, 3.78593890, 2.82842712, 3.53553390, 4.50924975, 6.36396103, 4.72581563),
    tolerance = 1e-7,
    scale = 1
  )
})

test_that("summarize_stddev() with key_columns works as expected", {
  ts_stddev <- summarize_stddev(
    multiple_simple_ts,
    column = "v",
    window = in_past("6s"),
    key_columns = c("id")
  ) %>% collect()

  expect_equal(ts_stddev$id, rep(c(0, 1), 6))
  expect_equal(
    ts_stddev$v_stddev,
    c(NaN, NaN, 0.707106781, NaN, 0.707106781, 0.707106781, 1.527525232, 1, 1.825741858, 1, 2.073644135, 1.707825128),
    tolerance = 1e-7,
    scale = 1
  )

  ts_stddev <- summarize_stddev(
    multiple_simple_ts,
    column = "v",
    window = in_future("6s"),
    key_columns = c("id")
  ) %>% collect()

  expect_equal(ts_stddev$id, rep(c(0, 1), 6))
  expect_equal(
    ts_stddev$v_stddev,
    c(2.073644135, 1.707825128, 1.707825128, 1.707825128, 1, 1.527525232, 1, 1.414213562, 0.707106781, Inf, NaN, Inf),
    tolerance = 1e-7,
    scale = 1
  )
})

test_that("summarize_var() works as expected", {
  ts_var <- summarize_var(
    ts,
    column = "v",
    window = in_past("6s")
  ) %>% collect()

  expect_equal(
    ts_var$v_variance,
    c(NaN, 18, 18, 14.33333333, 14.33333333, 8, 12.5, 20.33333333, 40.5, 22.33333333),
    tolerance = 1e-7,
    scale = 1
  )
})

test_that("summarize_var() with key_columns works as expected", {
  ts_var <- summarize_var(
    multiple_simple_ts,
    column = "v",
    window = in_past("6s"),
    key_columns = c("id")
  ) %>% collect()

  expect_equal(ts_var$id, rep(c(0, 1), 6))
  expect_equal(
    ts_var$v_variance,
    c(NaN, NaN, 0.5, NaN, 0.5, 0.5, 2.33333333, 1, 3.33333333, 1, 4.3, 2.91666667),
    tolerance = 1e-7,
    scale = 1
  )

  ts_var <- summarize_var(
    multiple_simple_ts,
    column = "v",
    window = in_future("6s"),
    key_columns = c("id")
  ) %>% collect()

  expect_equal(ts_var$id, rep(c(0, 1), 6))
  expect_equal(
    ts_var$v_variance,
    c(4.3, 2.91666667, 2.91666667, 2.91666667, 1, 2.33333333, 1, 2, 0.5, Inf, -Inf, Inf),
    tolerance = 1e-7,
    scale = 1
  )
})

test_that("summarize_covar() works as expected", {
  ts_covar <- summarize_covar(
    ts,
    xcolumn = "u",
    ycolumn = "v",
    window = in_past("6s")
  ) %>% collect()

  expect_equal(
    ts_covar$u_v_covariance,
    c(0, -7.5, -7.5, -6.3333333, -6.3333333, -4, 6.25, 17.8888889, 27, 22.4444444),
    tolerance = 1e-7,
    scale = 1
  )
})

test_that("summarize_covar() with key_columns works as expected", {
  ts_covar <- summarize_covar(
    multiple_simple_ts,
    xcolumn = "u",
    ycolumn = "v",
    window = in_past("6s"),
    key_columns = c("id")
  ) %>% collect()

  expect_equal(ts_covar$id, rep(c(0, 1), 6))
  expect_equal(
    ts_covar$u_v_covariance,
    c(0, NaN, -0.25, 0, -0.25, 3, -2.11111111, 1, -1.75, 1, -2.48, -1.5),
    tolerance = 1e-7,
    scale = 1
  )

  ts_covar <- summarize_covar(
    multiple_simple_ts,
    xcolumn = "u",
    ycolumn = "v",
    window = in_future("6s"),
    key_columns = c("id")
  ) %>% collect()

  expect_equal(ts_covar$id, rep(c(0, 1), 6))
  expect_equal(
    ts_covar$u_v_covariance,
    c(-2.48, -1.5, -1.3125, -1.5, 0, -5.11111111, 0, -1, -0.5, 0, 0, 0),
    tolerance = 1e-7,
    scale = 1
  )
})

test_that("summarize_weighted_covar() works as expected", {
  ts_weighted_covar <- summarize_weighted_covar(
    ts,
    xcolumn = "u",
    ycolumn = "v",
    window = in_past("6s"),
    weight_column = "w"
  ) %>% collect()

  expect_equal(
    ts_weighted_covar$u_v_w_weightedCovariance,
    c(NaN, -15, -12, -9.0526316, -8.0952381, -8, 12.5, 21.5384615, 54, 35.1428571),
    tolerance = 1e-7,
    scale = 1
  )
})

test_that("summarize_weighted_covar() with key_columns works as expected", {
  ts_weighted_covar <- summarize_weighted_covar(
    multiple_simple_ts,
    xcolumn = "u",
    ycolumn = "v",
    window = in_past("6s"),
    weight_column = "w",
    key_columns = c("id")
  ) %>% collect()

  expect_equal(ts_weighted_covar$id, rep(c(0, 1), 6))
  expect_equal(
    ts_weighted_covar$u_v_w_weightedCovariance,
    c(NaN, NaN, -0.5, NaN, -0.4, NaN, -3, NaN, -2.19565217, NaN, -1.93700787, NaN),
    tolerance = 1e-7,
    scale = 1
  )

  ts_weighted_covar <- summarize_weighted_covar(
    multiple_simple_ts,
    xcolumn = "u",
    ycolumn = "v",
    weight_column = "w",
    window = in_future("6s"),
    key_columns = c("id")
  ) %>% collect()

  expect_equal(ts_weighted_covar$id, rep(c(0, 1), 6))
  expect_equal(
    ts_weighted_covar$u_v_w_weightedCovariance,
    c(-3.2777777778, -1.2391304348, -1.1296296296, -1.2391304348, 0.0526315789, -7.3181818182, 0.0526315789, -2, -1, NaN, NaN, NaN),
    tolerance = 1e-7,
    scale = 1
  )
})

test_that("summarize_quantile() works as expected", {
  ts_quantile <- summarize_quantile(
    quantile_summarizer_test_case_ts,
    column = "v",
    p = c(0.25, 0.5, 0.75),
    window = in_past("5s")
  ) %>% collect()

  expect_equal(
    ts_quantile$`v_0.25quantile`,
    c(seq(1, 2.25, 0.25), seq(3.25, 16.25, 1))
  )

  expect_equal(
    ts_quantile$`v_0.5quantile`,
    c(seq(1, 3.5, 0.5), seq(4.5, 17.5, 1))
  )

  expect_equal(
    ts_quantile$`v_0.75quantile`,
    c(seq(1, 4.75, 0.75), seq(5.75, 18.75, 1))
  )
})

test_that("summarize_quantile() with key_columns works as expected", {
  ts_quantile <- summarize_quantile(
    quantile_summarizer_test_case_ts,
    column = "v",
    p = c(0.25, 0.5, 0.75),
    window = in_past("5s"),
    key_columns = c("c")
  ) %>% collect()

  for (x in range(5)) {
    expect_equal(
      ts_quantile %>%
        dplyr::filter(c == x) %>%
        dplyr::arrange(time) %>%
        dplyr::pull(v_0.25quantile),
      c(x, x + seq(1.25, 11.25, 5))
    )
    expect_equal(
      ts_quantile %>%
        dplyr::filter(c == x) %>%
        dplyr::arrange(time) %>%
        dplyr::pull(v_0.5quantile),
      c(x, x + seq(2.5, 12.5, 5))
    )
    expect_equal(
      ts_quantile %>%
        dplyr::filter(c == x) %>%
        dplyr::arrange(time) %>%
        dplyr::pull(v_0.75quantile),
      c(x, x + seq(3.75, 13.75, 5))
    )
  }
})
