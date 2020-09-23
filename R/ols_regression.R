#' @include summarizers.R
NULL

#' OLS regression
#'
#' Ordinary least squares regression
#'
#' @param ts_rdd Timeseries RDD containing dependent and independent variables
#' @param formula An object of class "formula" (or one that can be coerced
#'   to that class) which symbolically describes the model to be fitted, with
#'   the left-hand-side being the column name of the dependent variable, and
#'   the right-hand-side being column name(s) of independent variable(s)
#'   delimited by `+`, e.g., `mpg ~ hp + weight + am` for predicting `mpg` based
#'   on `hp`, `weight` and `am`
#' @param weight Name of the weight column if performing a weighted OLS
#'   regression, or NULL if otherwise. Default: NULL.
#' @param has_intercept Whether to include an intercept term (default: TRUE).
#'   If FALSE, then the resulting regression plane will always pass through the
#'   origin.
#' @param ignore_const_vars Whether to ignore independent variables that are
#'   constant or nearly constant based on const_threshold (default: FALSE).
#'   If TRUE, the scalar fields of regression result are the same as if the
#'   constant variables are not included as independent variables. The output
#'   beta, tStat, stdErr columns will still have the same dimension number of
#'   elements as the number of independent variables. However, entries
#'   corresponding to independent variables that are considered constant will
#'   have 0.0 for beta and stdErr; and Double.NaN for tStat.
#'   If FALSE and at least one independent variable is considered constant, the
#'   regression will output Double.NaN for all values. Note that if there are
#'   multiple independent variables that can be considered constant and if the
#'   resulting model should have an intercept term, then it is recommended to
#'   set both ignore_const_vars and has_intercept to TRUE.
#' @param const_var_threshold Consider an independent variable `x` as constant
#'   if ((number of observations) * variance(x)) is less than this value.
#'   Default: 1e-12.
#' @return A TimeSeries RDD with the following schema:
#'   * - "samples": [[LongType]], the number of samples
#'   * - "beta": [[ArrayType]] of [[DoubleType]], beta without the intercept
#'       component
#'   * - "intercept": [[DoubleType]], the intercept
#'   * - "hasIntercept": [[BooleanType]], whether the model has an intercept
#'       term
#'   * - "stdErr_intercept": [[DoubleType]], the standard error of the intercept
#'   * - "stdErr_beta": [[ArrayType]] of [[DoubleType]], the standard error of
#'       beta
#'   * - "rSquared": [[DoubleType]], the r-squared statistics
#'   * - "r": [[DoubleType]], the squared root of r-squared statistics
#'   * - "tStat_intercept": [[DoubleType]], the t-stats of the intercept
#'   * - "tStat_beta": [[ArrayType]] of [[DoubleType]], the t-stats of beta
#'   * - "logLikelihood": [[DoubleType]], the log-likelihood of the data given
#'       the fitted betas
#'   * - "akaikeIC": [[DoubleType]], the Akaike information criterion
#'   * - "bayesIC": [[DoubleType]], the Bayes information criterion
#'   * - "cond": [[DoubleType]], the condition number of the Gram matrix X^TX
#'       where X is the matrix formed by row vectors of independent variables
#'       (including a constant entry corresponding to the intercept if
#'       `has_intercept` is TRUE)
#'   * - "const_columns": [[ArrayType]] of [[StringType]], the list of
#'       independent variables that are considered constants
#'
#' @family summarizers
#'
#' @examples
#'
#' library(sparklyr)
#' library(sparklyr.flint)
#'
#' sc <- try_spark_connect(master = "local")
#'
#' if (!is.null(sc)) {
#'   mtcars_sdf <- copy_to(sc, mtcars, overwrite = TRUE) %>%
#'     dplyr::mutate(time = 0L)
#'   mtcars_ts <- from_sdf(mtcars_sdf, is_sorted = TRUE, time_unit = "SECONDS")
#'   model <- ols_regression(
#'     mtcars_ts, mpg ~ cyl + disp + hp + drat + wt + vs + am + gear + carb
#'   ) %>%
#'       collect()
#' } else {
#'   message("Unable to establish a Spark connection!")
#' }
#'
#' @export
ols_regression <- function(
                           ts_rdd,
                           formula,
                           weight = NULL,
                           has_intercept = TRUE,
                           ignore_const_vars = FALSE,
                           const_var_threshold = 1e-12) {
  independent_vars <- labels(terms(formula))
  dependent_var <- setdiff(all.vars(formula), independent_vars)
  if (length(dependent_var) > 1) {
    stop(
      "Formula must specify a single column as the dependent variable (",
      "e.g., `outcome ~ p + q + r + s` if a column named 'outcome' contains ",
      "the dependent variable being modeled)."
    )
  }

  summarizer_args <- list(
    method = "OLSRegression",
    dependent_var,
    as.list(independent_vars),
    weight,
    has_intercept,
    ignore_const_vars,
    const_var_threshold
  )

  summarize(ts_rdd, summarizer_args)
}
