context("sdf-utils")

sc <- testthat_spark_connection()
ts <- fromSDF(
  testthat_generic_time_series(),
  is_sorted = FALSE,
  time_unit = "SECONDS",
  time_column = "t"
)
simple_ts <- fromSDF(
  testthat_simple_time_series(),
  is_sorted = TRUE,
  time_unit = "SECONDS",
  time_column = "t"
)
corr_test_case_ts <- fromSDF(
  testthat_corr_test_case(),
  is_sorted = TRUE,
  time_unit = "SECONDS",
  time_column = "t"
)

test_that("summarize_count() works as expected", {
  ts_count <- summarize_count(ts, in_past("3s")) %>% collect()

  expect_equal(ts_count$count, c(1, 2, 3, 3, 3, 2, 1, 2, 3, 3))
})

test_that("summarize_count() with specific column works as expected", {
  ts_count <- summarize_count(ts, in_past("3s"), column = "v") %>% collect()

  expect_equal(ts_count$v_count, c(1, 2, 2, 2, 1, 1, 1, 2, 2, 2))
})

test_that("summarize_min() works as expected", {
  ts_min <- summarize_min(ts, in_past("3s"), column = "v") %>% collect()

  expect_equal(ts_min$v_min, c(4, -2, -2, -2, 5, 1, -4, -4, -4, 3))
})

test_that("summarize_max() works as expected", {
  ts_max <- summarize_max(ts, in_past("3s"), column = "v") %>% collect()

  expect_equal(ts_max$v_max, c(4, 4, 4, 5, 5, 1, -4, 5, 5, 5))
})

test_that("summarize_sum() works as expected", {
  ts_sum <- summarize_sum(ts, in_past("3s"), column = "v") %>% collect()

  expect_equal(ts_sum$v_sum, c(4, 2, 2, 3, 5, 1, -4, 1, 1, 8))
})

test_that("summarize_avg() works as expected", {
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

test_that("summarize_weighted_avg() works as expected", {
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

test_that("summarize_stddev() works as expected", {
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

test_that("summarize_var() works as expected", {
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

test_that("summarize_covar() works as expected", {
  ts_covar <- summarize_covar(
    ts,
    in_past("6s"),
    xcolumn = "u",
    ycolumn = "v"
  ) %>% collect()

  expect_equal(
    ts_covar$u_v_covariance,
    c(0, -7.5, -7.5, -6.3333333, -6.3333333, -4, 6.25, 17.8888889, 27, 22.4444444),
    tolerance = 1e-7,
    scale = 1
  )
})

test_that("summarize_weighted_covar() works as expected", {
  ts_weighted_covar <- summarize_weighted_covar(
    ts,
    in_past("6s"),
    xcolumn = "u",
    ycolumn = "v",
    weight_column = "w"
  ) %>% collect()

  expect_equal(
    ts_weighted_covar$u_v_w_weightedCovariance,
    c(NaN, -15, -12, -9.0526316, -8.0952381, -8, 12.5, 21.5384615, 54, 35.1428571),
    tolerance = 1e-7,
    scale = 1
  )
})

test_that("summarize_z_score() works as expected", {
  ts_in_sample_z_score <- summarize_z_score(simple_ts, "v", TRUE) %>% collect()
  expect_equal(
    ts_in_sample_z_score$v_zScore,
    1.52542554,
    tolerance = 1e-7,
    scale = 1
  )

  ts_out_of_sample_z_score <-
    summarize_z_score(simple_ts, "v", FALSE) %>% collect()
  expect_equal(
    ts_out_of_sample_z_score$v_zScore,
    1.80906807,
    tolerance = 1e-7,
    scale = 1
  )
})

test_that("summarize_nth_moment() works as expected", {
  ts_0th_moment <- summarize_nth_moment(simple_ts, "v", 0) %>% collect()
  expect_equal(ts_0th_moment$v_0thMoment, 1)

  ts_1st_moment <- summarize_nth_moment(simple_ts, "v", 1) %>% collect()
  expect_equal(ts_1st_moment$v_1thMoment, 3.25)

  ts_2nd_moment <- summarize_nth_moment(simple_ts, "v", 2) %>% collect()
  expect_equal(
    ts_2nd_moment$v_2thMoment,
    13.54166667,
    tolerance = 1e-7,
    scale = 1
  )

  ts_3rd_moment <- summarize_nth_moment(simple_ts, "v", 3) %>% collect()
  expect_equal(ts_3rd_moment$v_3thMoment, 63.375)
})

test_that("summarize_nth_central_moment() works as expected", {
  ts_1st_central_moment <- summarize_nth_central_moment(simple_ts, "v", 1) %>% collect()
  expect_equal(ts_1st_central_moment$v_1thCentralMoment, 0)

  ts_2nd_central_moment <- summarize_nth_central_moment(simple_ts, "v", 2) %>% collect()
  expect_equal(
    ts_2nd_central_moment$v_2thCentralMoment,
    2.97916667,
    tolerance = 1e-7,
    scale = 1
  )

  ts_3rd_central_moment <- summarize_nth_central_moment(simple_ts, "v", 3) %>% collect()
  expect_equal(ts_3rd_central_moment$v_3thCentralMoment, 0)

  ts_4th_central_moment <- summarize_nth_central_moment(simple_ts, "v", 4) %>% collect()
  expect_equal(
    ts_4th_central_moment$v_4thCentralMoment,
    15.82682292,
    tolerance = 1e-7,
    scale = 1
  )
})

test_that("summarize_corr() works as expected", {
  ts_corr <- summarize_corr(corr_test_case_ts, c("p", "sp")) %>% collect()
  expect_equal(ts_corr$p_sp_correlation, 1)
  expect_equal(ts_corr$p_sp_correlationTStat, Inf)

  ts_corr <- summarize_corr(corr_test_case_ts, c("p", "np")) %>% collect()
  expect_equal(ts_corr$p_np_correlation, -1)
  expect_equal(ts_corr$p_np_correlationTStat, -Inf)

  ts_corr <- summarize_corr(corr_test_case_ts, c("p", "dp")) %>% collect()
  expect_equal(ts_corr$p_dp_correlation, 1)
  expect_equal(ts_corr$p_dp_correlationTStat, Inf)

  ts_corr <- summarize_corr(corr_test_case_ts, c("p", "z")) %>% collect()
  expect_true(is.nan(ts_corr$p_z_correlation))
  expect_true(is.nan(ts_corr$p_z_correlationTStat))

  ts_corr <- summarize_corr(corr_test_case_ts, c("p", "f")) %>% collect()
  expect_equal(
    ts_corr$p_f_correlation,
    -0.02189612,
    tolerance = 1e-7,
    scale = 1
  )
  expect_equal(
    ts_corr$p_f_correlationTStat,
    -0.04380274,
    tolerance = 1e-7,
    scale = 1
  )
})

test_that("summarize_corr2() works as expected", {
  ts_corr <- summarize_corr2(corr_test_case_ts, c("p", "np"), c("f", "dp")) %>%
    collect()
  expect_equal(
    ts_corr$p_f_correlation,
    -0.02189612,
    tolerance = 1e-7,
    scale = 1
  )
  expect_equal(
    ts_corr$p_f_correlationTStat,
    -0.04380274,
    tolerance = 1e-7,
    scale = 1
  )
  expect_equal(ts_corr$p_dp_correlation, 1)
  expect_equal(ts_corr$p_dp_correlationTStat, Inf)
  expect_equal(
    ts_corr$np_f_correlation,
    0.02189612,
    tolerance = 1e-7,
    scale = 1
  )
  expect_equal(
    ts_corr$np_f_correlationTStat,
    0.04380274,
    tolerance = 1e-7,
    scale = 1
  )
  expect_equal(ts_corr$np_dp_correlation, -1)
  expect_equal(ts_corr$np_dp_correlationTStat, -Inf)
})

test_that("summarize_weighted_corr() works as expected", {
  weighted_corr_test_case_ts <- fromSDF(
    testthat_weighted_corr_test_case(),
    is_sorted = TRUE,
    time_unit = "SECONDS",
    time_column = "t"
  )
  ts_weighted_corr <- summarize_weighted_corr(
    weighted_corr_test_case_ts,
    "x",
    "y",
    "w"
  ) %>% collect()

  expect_equal(ts_weighted_corr$x_y_w_weightedCorrelation, -1)
})
