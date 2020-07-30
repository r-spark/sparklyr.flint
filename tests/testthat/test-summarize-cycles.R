context("summarize-cycles")

sc <- testthat_spark_connection()
ts <- fromSDF(
  testthat_generic_cycles(),
  is_sorted = FALSE,
  time_unit = "SECONDS",
  time_column = "t"
)
quantile_summarizer_test_case_ts <- fromSDF(
  testthat_quantile_summarizer_test_case(),
  is_sorted = FALSE,
  time_unit = "SECONDS",
  time_column = "c"
)

verify_timestamps <- function(df) {
  expect_equal(as.numeric(df$time), seq(3))
}

verify_attrs_with_id_key_column <- function(df) {
  expect_equal(df$id, c(1, 2, 0, 1, 2, 3))
  expect_equal(as.numeric(df$time), c(1, 1, 2, 2, 3, 3))
}

# test_that("summarize_count() works as expected", {
#   ts_count <- summarize_count(ts) %>% collect()
#
#   verify_timestamps(ts_count)
#   expect_equal(ts_count$count, c(3, 4, 3))
# })
#
# test_that("summarize_count() with specific column works as expected", {
#   ts_count <- summarize_count(ts, column = "v") %>% collect()
#
#   verify_timestamps(ts_count)
#   expect_equal(ts_count$v_count, c(2, 3, 2))
# })
#
# test_that("summarize_count() with key_columns works as expected", {
#   ts_count <- summarize_count(ts, key_columns = c("id")) %>%
#     collect() %>%
#     dplyr::arrange(time, id)
#
#   verify_attrs_with_id_key_column(ts_count)
#   expect_equal(ts_count$count, c(2, 1, 2, 2, 1, 2))
# })
#
# test_that("summarize_count() with key_columns and specific column works as expected", {
#   ts_count <- summarize_count(ts, column = "v", key_columns = c("id")) %>%
#     collect()
#
#   verify_attrs_with_id_key_column(ts_count)
#   expect_equal(ts_count$v_count, c(1, 1, 1, 2, 1, 1))
# })
#
# test_that("summarize_min() works as expected", {
#   ts_min <- summarize_min(ts, column = "v") %>% collect()
#
#   verify_timestamps(ts_min)
#   expect_equal(ts_min$v_min, c(-2, -4, 3))
# })
#
# test_that("summarize_min() with key_columns works as expected", {
#   ts_min <- summarize_min(ts, column = "v", key_columns = c("id")) %>%
#     collect() %>%
#     dplyr::arrange(time, id)
#
#   verify_attrs_with_id_key_column(ts_min)
#   expect_equal(ts_min$v_min, c(4, -2, 5, -4, 5, 3))
# })
#
# test_that("summarize_max() works as expected", {
#   ts_max <- summarize_max(ts, column = "v") %>% collect()
#
#   verify_timestamps(ts_max)
#   expect_equal(ts_max$v_max, c(4, 5, 5))
# })
#
# test_that("summarize_max() with key_columns works as expected", {
#   ts_max <- summarize_max(ts, column = "v", key_columns = c("id")) %>%
#     collect() %>%
#     dplyr::arrange(time, id)
#
#   verify_attrs_with_id_key_column(ts_max)
#   expect_equal(ts_max$v_max, c(4, -2, 5, 1, 5, 3))
# })
#
# test_that("summarize_sum() works as expected", {
#   ts_sum <- summarize_sum(ts, column = "v") %>% collect()
#
#   verify_timestamps(ts_sum)
#   expect_equal(ts_sum$v_sum, c(2, 2, 8))
# })
#
# test_that("summarize_sum() with key_columns works as expected", {
#   ts_sum <- summarize_sum(ts, column = "v", key_columns = c("id")) %>%
#     collect() %>%
#     dplyr::arrange(time, id)
#
#   verify_attrs_with_id_key_column(ts_sum)
#   expect_equal(ts_sum$v_sum, c(4, -2, 5, -3, 5, 3))
# })
#
# test_that("summarize_product() works as expected", {
#   ts_product <- summarize_product(ts, column = "v") %>% collect()
#
#   verify_timestamps(ts_product)
#   expect_equal(ts_product$v_product, c(-8, -20, 15))
# })
#
# test_that("summarize_product() with key_columns works as expected", {
#   ts_product <- summarize_product(ts, column = "v", key_columns = c("id")) %>%
#     collect() %>%
#     dplyr::arrange(time, id)
#
#   verify_attrs_with_id_key_column(ts_product)
#   expect_equal(ts_product$v_product, c(4, -2, 5, -4, 5, 3))
# })
#
# test_that("summarize_avg() works as expected", {
#   ts_avg <- summarize_avg(ts, column = "v") %>% collect()
#
#   verify_timestamps(ts_avg)
#   expect_equal(ts_avg$v_mean, c(1, 0.66666667, 4), tolerance = 1e-7, scale = 1)
# })
#
# test_that("summarize_avg() with key_columns works as expected", {
#   ts_avg <- summarize_avg(ts, column = "v", key_columns = c("id")) %>%
#     collect() %>%
#     dplyr::arrange(time, id)
#
#   verify_attrs_with_id_key_column(ts_avg)
#   expect_equal(ts_avg$v_mean, c(4, -2, 5, -1.5, 5, 3))
# })
#
# test_that("summarize_weighted_avg() works as expected", {
#   ts_weighted_avg <- summarize_weighted_avg(
#     ts,
#     column = "v",
#     weight_column = "w"
#   ) %>% collect()
#
#   verify_timestamps(ts_weighted_avg)
#   expect_equal(ts_weighted_avg$v_w_weightedMean, c(2, 0.6, 3.4))
#   expect_equal(
#     ts_weighted_avg$v_w_weightedStandardDeviation,
#     c(4.2426407, 5.0373604, 1.4142136),
#     tolerance = 1e-7,
#     scale = 1
#   )
# })
#
# test_that("summarize_weighted_avg() with key_columns works as expected", {
#   ts_weighted_avg <- summarize_weighted_avg(
#     ts,
#     column = "v",
#     weight_column = "w",
#     key_columns = c("id")
#   ) %>%
#     collect() %>%
#     dplyr::arrange(time, id)
#
#   verify_attrs_with_id_key_column(ts_weighted_avg)
#   expect_equal(
#     ts_weighted_avg$v_w_weightedMean,
#     c(4, -2, 5, -2.33333333, 5,  3),
#     tolerance = 1e-7,
#     scale = 1
#   )
#   expect_equal(
#     ts_weighted_avg$v_w_weightedStandardDeviation,
#     c(NaN, NaN, NaN, 3.53553391, NaN, NaN),
#     tolerance = 1e-7,
#     scale = 1
#   )
# })
#
# test_that("summarize_stddev() works as expected", {
#   ts_stddev <- summarize_stddev(
#     ts,
#     column = "v"
#   ) %>% collect()
#
#   verify_timestamps(ts_stddev)
#   expect_equal(
#     ts_stddev$v_stddev,
#     c(4.24264069, 4.50924975, 1.41421356),
#     tolerance = 1e-7,
#     scale = 1
#   )
# })
#
# test_that("summarize_stddev() with key_columns works as expected", {
#   ts_stddev <- summarize_stddev(
#     ts,
#     column = "v",
#     key_columns = c("id")
#   ) %>%
#     collect() %>%
#     dplyr::arrange(time, id)
#
#   verify_attrs_with_id_key_column(ts_stddev)
#   expect_equal(
#     ts_stddev$v_stddev,
#     c(NaN, NaN, NaN, 3.53553391, NaN, NaN),
#     tolerance = 1e-7,
#     scale = 1
#   )
# })
#
# test_that("summarize_var() works as expected", {
#   ts_var <- summarize_var(
#     ts,
#     column = "v"
#   ) %>% collect()
#
#   verify_timestamps(ts_var)
#   expect_equal(
#     ts_var$v_variance,
#     c(18, 20.3333333, 2),
#     tolerance = 1e-7,
#     scale = 1
#   )
# })
#
# test_that("summarize_var() with key_columns works as expected", {
#   ts_var <- summarize_var(
#     ts,
#     column = "v",
#     key_columns = c("id")
#   ) %>%
#     collect() %>%
#     dplyr::arrange(time, id)
#
#   verify_attrs_with_id_key_column(ts_var)
#   expect_equal(
#     ts_var$v_variance,
#     c(NaN, NaN, NaN, 12.5, NaN, NaN),
#     tolerance = 1e-7,
#     scale = 1
#   )
# })
#
# test_that("summarize_covar() works as expected", {
#   ts_covar <- summarize_covar(
#     ts,
#     xcolumn = "u",
#     ycolumn = "v"
#   ) %>% collect()
#
#   verify_timestamps(ts_covar)
#   expect_equal(ts_covar$u_v_covariance, c(-7.5, 2, -1))
# })
#
# test_that("summarize_covar() with key_columns works as expected", {
#   ts_covar <- summarize_covar(
#     ts,
#     xcolumn = "u",
#     ycolumn = "v",
#     key_columns = c("id")
#   ) %>%
#     collect() %>%
#     dplyr::arrange(time, id)
#
#   verify_attrs_with_id_key_column(ts_covar)
#   expect_equal(ts_covar$u_v_covariance, c(0, 0, 0, 6.25, 0, 0))
# })
#
# test_that("summarize_weighted_covar() works as expected", {
#   ts_weighted_covar <- summarize_weighted_covar(
#     ts,
#     xcolumn = "u",
#     ycolumn = "v",
#     weight_column = "w"
#   ) %>% collect()
#
#   expect_equal(ts_weighted_covar$u_v_w_weightedCovariance, c(-15, 3.375, -2))
# })
#
# test_that("summarize_weighted_covar() with key_columns works as expected", {
#   ts_weighted_covar <- summarize_weighted_covar(
#     ts,
#     xcolumn = "u",
#     ycolumn = "v",
#     weight_column = "w",
#     key_columns = c("id")
#   ) %>%
#     collect() %>%
#     dplyr::arrange(time, id)
#
#   verify_attrs_with_id_key_column(ts_weighted_covar)
#   expect_equal(
#     ts_weighted_covar$u_v_w_weightedCovariance,
#     c(NaN, NaN, NaN, 12.5, NaN, NaN)
#   )
# })
#
# test_that("summarize_quantile() works as expected", {
#   ts_quantile <- summarize_quantile(
#     quantile_summarizer_test_case_ts,
#     column = "v",
#     p = c(0.25, 0.5, 0.75)
#   ) %>% collect()
#
#   expect_equal(ts_quantile$v_0.25quantile, seq(4.75, 8.75, 1))
#   expect_equal(ts_quantile$v_0.5quantile, seq(8.5, 12.5, 1))
#   expect_equal(ts_quantile$v_0.75quantile, seq(12.25, 16.25, 1))
# })
#
# test_that("summarize_quantile() with key_columns works as expected", {
#   ts_quantile <- summarize_quantile(
#     quantile_summarizer_test_case_ts,
#     column = "v",
#     p = c(0.25, 0.5, 0.75),
#     key_columns = c("id")
#   ) %>% collect()
#
#   for (x in c(0, 1)) {
#     expect_equal(
#       ts_quantile %>%
#         dplyr::filter(id == x) %>%
#         dplyr::arrange(time) %>%
#         dplyr::pull(v_0.25quantile),
#       x * 10 + seq(2.25, 6.25, 1)
#     )
#     expect_equal(
#       ts_quantile %>%
#         dplyr::filter(id == x) %>%
#         dplyr::arrange(time) %>%
#         dplyr::pull(v_0.5quantile),
#       x * 10 + seq(3.5, 7.5, 1)
#     )
#     expect_equal(
#       ts_quantile %>%
#         dplyr::filter(id == x) %>%
#         dplyr::arrange(time) %>%
#         dplyr::pull(v_0.75quantile),
#       x * 10 + seq(4.75, 8.75, 1)
#     )
#   }
# })

test_that("summarize_dot_product() works as expected", {
  ts_dot_product <- summarize_dot_product(
    ts,
    xcolumn = "u",
    ycolumn = "v"
  ) %>% collect()

  expect_equal(ts_dot_product$u_v_dotProduct, c(-18, 2, 70))
})

test_that("summarize_dot_product() with key_columns works as expected", {
  ts_dot_product <- summarize_dot_product(
    ts,
    xcolumn = "u",
    ycolumn = "v",
    key_columns = c("id")
  ) %>% collect()

  verify_attrs_with_id_key_column(ts_dot_product)
  expect_equal(ts_dot_product$u_v_dotProduct, c(-16, -2, -15, 17, 40, 30))
})
