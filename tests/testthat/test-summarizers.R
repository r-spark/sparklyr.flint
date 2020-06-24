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
  ts_count <- summarize_sum(ts, in_past("3s"), column = "v") %>% collect()

  expect_equal(ts_count$v_sum, c(4, 2, 2, 3, 5, 1, -4, 1, 1, 8))
})
