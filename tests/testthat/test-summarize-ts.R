context("summarize-ts")

sc <- testthat_spark_connection()
simple_ts <- fromSDF(
  testthat_simple_time_series(),
  is_sorted = TRUE,
  time_unit = "SECONDS",
  time_column = "t"
)
multiple_simple_ts <- fromSDF(
  testthat_multiple_simple_ts_test_case(),
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
corr_test_case_multiple_ts <- fromSDF(
  testthat_corr_multiple_ts_test_case(),
  is_sorted = TRUE,
  time_unit = "SECONDS",
  time_column = "t"
)
weighted_corr_test_case_ts <- fromSDF(
  testthat_weighted_corr_test_case(),
  is_sorted = TRUE,
  time_unit = "SECONDS",
  time_column = "t"
)

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

test_that("summarize_z_score() with key_columns works as expected", {
  ts_in_sample_z_score <- summarize_z_score(
    multiple_simple_ts,
    "v",
    include_current_observation = TRUE,
    key_columns = c("id")
  ) %>%
    collect() %>%
    dplyr::arrange(id)

  expect_equal(ts_in_sample_z_score$id, c(0, 1))
  expect_equal(
    ts_in_sample_z_score$v_zScore,
    c(-1.15738277, 1.31746510),
    tolerance = 1e-7,
    scale = 1
  )

  ts_in_sample_z_score <- summarize_z_score(
    multiple_simple_ts,
    "v",
    include_current_observation = FALSE,
    key_columns = c("id")
  ) %>%
    collect() %>%
    dplyr::arrange(id)

  expect_equal(ts_in_sample_z_score$id, c(0, 1))
  expect_equal(
    ts_in_sample_z_score$v_zScore,
    c(-1.64316767, 3),
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

test_that("summarize_nth_moment() with key_columns works as expected", {
  ts_0th_moment <- summarize_nth_moment(
    multiple_simple_ts,
    "v",
    0,
    key_columns = c("id")
  ) %>%
    collect() %>%
    dplyr::arrange(id)
  expect_equal(ts_0th_moment$id, c(0, 1))
  expect_equal(ts_0th_moment$v_0thMoment, c(1, 1))

  ts_1st_moment <- summarize_nth_moment(
    multiple_simple_ts,
    "v",
    1,
    key_columns = c("id")
  ) %>%
    collect() %>%
    dplyr::arrange(id)
  expect_equal(ts_1st_moment$id, c(0, 1))
  expect_equal(ts_1st_moment$v_1thMoment, c(3.4, 3.75))

  ts_2nd_moment <- summarize_nth_moment(
    multiple_simple_ts,
    "v",
    2,
    key_columns = c("id")
  ) %>%
    collect() %>%
    dplyr::arrange(id)
  expect_equal(
    ts_2nd_moment$v_2thMoment,
    c(15, 16.25),
    tolerance = 1e-7,
    scale = 1
  )

  ts_3rd_moment <- summarize_nth_moment(
    multiple_simple_ts,
    "v",
    3,
    key_columns = c("id")
  ) %>%
    collect() %>%
    dplyr::arrange(id)
  expect_equal(
    ts_3rd_moment$v_3thMoment,
    c(75.4, 78.75),
    tolerance = 1e-7,
    scale = 1
  )
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

test_that("summarize_nth_central_moment() with key_columns works as expected", {
  ts_1st_central_moment <- summarize_nth_central_moment(
    multiple_simple_ts,
    "v",
    1,
    key_columns = c("id")
  ) %>%
    collect() %>%
    dplyr::arrange(id)
  expect_equal(ts_1st_central_moment$id, c(0, 1))
  expect_equal(ts_1st_central_moment$v_1thCentralMoment, c(0, 0))

  ts_2nd_central_moment <- summarize_nth_central_moment(
    multiple_simple_ts,
    "v",
    2,
    key_columns = c("id")
  ) %>%
    collect() %>%
    dplyr::arrange(id)
  expect_equal(
    ts_2nd_central_moment$v_2thCentralMoment,
    c(3.440, 2.1875),
    tolerance = 1e-7,
    scale = 1
  )

  ts_3rd_central_moment <- summarize_nth_central_moment(
    multiple_simple_ts,
    "v",
    3,
    key_columns = c("id")
  ) %>%
    collect() %>%
    dplyr::arrange(id)
  expect_equal(
    ts_3rd_central_moment$v_3thCentralMoment,
    c(1.008, 1.40625),
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

test_that("summarize_corr() with key_columns works as expected", {
  ts_corr <- summarize_corr(
    corr_test_case_multiple_ts,
    c("p", "sp"),
    key_columns = c("id")) %>%
    collect() %>%
    dplyr::arrange(id)
  expect_equal(ts_corr$id, c(3, 7))
  expect_equal(ts_corr$p_sp_correlation, c(1, 1))

  ts_corr <- summarize_corr(
    corr_test_case_multiple_ts,
    c("p", "np"),
    key_columns = c("id")) %>%
    collect() %>%
    dplyr::arrange(id)
  expect_equal(ts_corr$id, c(3, 7))
  expect_equal(ts_corr$p_np_correlation, c(-1, -1))

  ts_corr <- summarize_corr(
    corr_test_case_multiple_ts,
    c("p", "dp"),
    key_columns = c("id")) %>%
    collect() %>%
    dplyr::arrange(id)
  expect_equal(ts_corr$id, c(3, 7))
  expect_equal(ts_corr$p_dp_correlation, c(1, 1))

  ts_corr <- summarize_corr(
    corr_test_case_multiple_ts,
    c("p", "z"),
    key_columns = c("id")) %>%
    collect() %>%
    dplyr::arrange(id)
  expect_equal(ts_corr$id, c(3, 7))
  expect_equal(ts_corr$p_z_correlation, c(NaN, NaN))

  ts_corr <- summarize_corr(
    corr_test_case_multiple_ts,
    c("p", "f"),
    key_columns = c("id")) %>%
    collect() %>%
    dplyr::arrange(id)
  expect_equal(ts_corr$id, c(3, 7))
  expect_equal(
    ts_corr$p_f_correlation,
    c(-0.47908486, -0.02189612),
    tolerance = 1e-7,
    scale = 1
  )
  expect_equal(
    ts_corr$p_f_correlationTStat,
    c(-1.09159718, -0.04380274),
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

test_that("summarize_corr2() with key_columns works as expected", {
  ts_corr <- summarize_corr2(
    corr_test_case_multiple_ts,
    c("p", "np"), c("f"),
    key_columns = c("id")) %>%
    collect() %>%
    dplyr::arrange(id)

  expect_equal(ts_corr$id, c(3, 7))
  expect_equal(
    ts_corr$p_f_correlation,
    c(-0.4790848587, -0.0218961214),
    tolerance = 1e-7,
    scale = 1
  )
  expect_equal(
    ts_corr$p_f_correlationTStat,
    c(-1.0915971793, -0.0438027444),
    tolerance = 1e-7,
    scale = 1
  )
  expect_equal(
    ts_corr$np_f_correlation,
    c(0.4790848587, 0.0218961214),
    tolerance = 1e-7,
    scale = 1
  )
  expect_equal(
    ts_corr$np_f_correlationTStat,
    c(1.0915971793, 0.0438027444),
    tolerance = 1e-7,
    scale = 1
  )
})

test_that("summarize_weighted_corr() works as expected", {
  ts_weighted_corr <- summarize_weighted_corr(
    weighted_corr_test_case_ts,
    "x",
    "y",
    "w"
  ) %>% collect()

  expect_equal(ts_weighted_corr$x_y_w_weightedCorrelation, -1)
})

test_that("summarize_weighted_corr() with key_columns works as expected", {
  ts_weighted_corr <- summarize_weighted_corr(
    weighted_corr_test_case_ts,
    "x",
    "y",
    "w",
    key_columns = c("id")
  ) %>%
    collect() %>%
    dplyr::arrange(id)

  expect_equal(ts_weighted_corr$id, c(0, 1))
  expect_equal(ts_weighted_corr$x_y_w_weightedCorrelation, c(-1, -1))
})
