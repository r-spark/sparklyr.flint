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

  expect_equal(as.numeric(ts_count$time), c(1, 1, 2, 2, 3, 3))
  expect_equal(ts_count$id, c(1, 2, 0, 1, 2, 3))
  expect_equal(ts_count$count, c(2, 1, 2, 2, 1, 2))
})

test_that("summarize_count() with key_columns and specific column works as expected", {
  ts_count <- summarize_count(ts, column = "v", key_columns = c("id")) %>%
    collect()

  expect_equal(as.numeric(ts_count$time), c(1, 1, 2, 2, 3, 3))
  expect_equal(ts_count$id, c(1, 2, 0, 1, 2, 3))
  expect_equal(ts_count$v_count, c(1, 1, 1, 2, 1, 1))
})
