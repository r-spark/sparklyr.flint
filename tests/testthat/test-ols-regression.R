context("ols-regression")

sc <- testthat_spark_connection()
sdf <- spark_read_csv(
  sc,
  path = file.path("data", "ols_regression_test_data.csv"),
  header = TRUE
)
ts <- from_sdf(sdf, is_sorted = TRUE, time_unit = "SECONDS")
sdf_singular <- sdf %>%
  dplyr::mutate(x3 = dplyr::sql("CAST(0 AS DOUBLE)"))
ts_singular <- from_sdf(sdf_singular, is_sorted = TRUE, time_unit = "SECONDS")
sdf_const <- sdf %>%
  dplyr::mutate(x3 = dplyr::sql("CAST(2 AS DOUBLE)"))
ts_const <- from_sdf(sdf_const, is_sorted = TRUE, time_unit = "SECONDS")

test_that("OLS regression works correctly with has_intercept = TRUE", {
  model <- ols_regression(
    ts, y ~ x1 + x2, weight = "w", has_intercept = TRUE
  ) %>%
    collect()

  expect_equal(model$intercept, 3.117181999992637, tolerance = 1e-12, scale = 1)
  expect_true(model$hasIntercept)
  expect_equal(model$samples, spark_jobj(ts) %>% invoke("count"))
  expect_equal(model$r, 0.23987985194607062, tolerance = 1e-12, scale = 1)
  expect_equal(model$rSquared, 0.05754234336966876, tolerance = 1e-12, scale = 1)
  expect_equal(model$stdErr_intercept, 0.5351305295407137, tolerance = 1e-12, scale = 1)
  expect_equal(model$tStat_intercept, 5.825087203804313, tolerance = 1e-12, scale = 1)
  expect_equal(model$cond, 1.4264121300439514, tolerance = 1e-12, scale = 1)
  expect_equal(model$logLikelihood, -312.11292022635649, tolerance = 1e-12, scale = 1)
  expect_equal(model$akaikeIC, 630.225840453, tolerance = 1e-8, scale = 1)
  expect_equal(model$bayesIC, 638.041351011, tolerance = 1e-8, scale = 1)
  expect_equal(
    model$beta,
    list(c(0.28007101558427594, 1.3162178418611101)),
    tolerance = 1e-12,
    scale = 1
  )
  expect_equal(
    model$stdErr_beta,
    list(c(0.5870869011202909, 0.5582749581661886)),
    tolerance = 1e-12,
    scale = 1
  )
  expect_equal(
    model$tStat_beta,
    list(c(0.4770520600099199, 2.3576515883581814)),
    tolerance = 1e-12,
    scale = 1
  )
})

test_that("OLS regression works correctly with has_intercept = FALSE", {
  model <- ols_regression(
    ts, y ~ x1 + x2, weight = "w", has_intercept = FALSE
  ) %>%
    collect()

  expect_equal(model$intercept, 0)
  expect_false(model$hasIntercept)
  expect_equal(model$samples, spark_jobj(ts) %>% invoke("count"))
  expect_equal(model$r, 0.19129580479059843, tolerance = 1e-12, scale = 1)
  expect_equal(model$rSquared, 0.036594084930482745, tolerance = 1e-12, scale = 1)
  expect_equal(model$cond, 1.1509375418, tolerance = 1e-12, scale = 1)
  expect_equal(model$logLikelihood, -327.11113940398695, tolerance = 1e-12, scale = 1)
  expect_equal(model$akaikeIC, 658.222278808, tolerance = 1e-8, scale = 1)
  expect_equal(model$bayesIC, 663.43261918, tolerance = 1e-8, scale = 1)
  expect_equal(
    model$beta,
    list(c(-0.18855696254850499, 1.2397406248059233)),
    tolerance = 1e-12,
    scale = 1
  )
  expect_equal(
    model$stdErr_beta,
    list(c(0.672195067165334, 0.6451152214049083)),
    tolerance = 1e-12,
    scale = 1
  )
  expect_equal(
    model$tStat_beta,
    list(c(-0.28050929225597476, 1.9217351934528257)),
    tolerance = 1e-12,
    scale = 1
  )
})

test_that("OLS regression returns NaN beta for singular matrix", {
  model <- ols_regression(
    ts_singular, y ~ x1 + x2 + x3, weight = "w", has_intercept = FALSE
  ) %>%
    collect()

  expect_equal(model$beta, list(c(NaN, NaN, NaN)))
  expect_true(is.nan(model$cond))
  expect_equal(model$const_columns, list(list("x3")))
})

test_that("OLS regression does not return NaN beta for almost-constant column", {
  for (tgt in c(1L, 26L, 51L, 76L, 100L)) {
    sql <- sprintf("IF(row_number() OVER (ORDER BY NULL) == %d, 1, 0)", tgt)
    sdf_almost_const <- sdf %>%
      dplyr::mutate(x3 = dplyr::sql(sql))
    ts_almost_const <- from_sdf(
      sdf_almost_const, is_sorted = TRUE, time_unit = "SECONDS"
    )
    model <- ols_regression(
      ts_almost_const,
      y ~ x1 + x2 + x3,
      weight = "w",
      has_intercept = FALSE,
      ignore_const_vars = TRUE
    ) %>%
      collect()
    for (x in seq_along(model$beta[[1]])) {
      expect_false(is.nan(model$beta[[1]][[x]]))
    }
  }
})

test_that("OLS regression works as expected with ignore_const_vars = TRUE", {
  for (has_intercept in c(TRUE, FALSE)) {
    model1 <- ols_regression(
      ts_const,
      y ~ x1 + x2,
      weight = "w",
      has_intercept = has_intercept,
      ignore_const_vars = FALSE
    ) %>%
      collect()
    model2 <- ols_regression(
      ts_const,
      y ~ x1 + x3 + x2,
      weight = "w",
      has_intercept = has_intercept,
      ignore_const_vars = TRUE
    ) %>%
      collect()

    for (column in c(
                     "intercept", "hasIntercept", "samples", "r", "rSquared",
                     "stdErr_intercept", "tStat_intercept", "logLikelihood",
                     "akaikeIC", "bayesIC", "cond"
                    )) {
      expect_false(is.null(model1[[column]]))
      expect_false(is.null(model2[[column]]))
      expect_equal(model1[[column]], model2[[column]])
    }
    for (column in c("beta", "stdErr_beta", "tStat_beta")) {
      expect_equal(model1[[column]][[1]][[1]], model2[[column]][[1]][[1]])
      expect_equal(
        model2[[column]][[1]][[2]],
        if (identical(column, "tStat_beta")) {
          NaN
        } else {
          0
        }
      )
      expect_equal(model1[[column]][[1]][[2]], model2[[column]][[1]][[3]])
    }
  }
})
