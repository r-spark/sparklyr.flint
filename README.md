
Sparklyr.flint is a sparklyr extension making Flint time series library
functionalities (<https://github.com/twosigma/flint>) accessible through
R.

This extension is currently under active development. It requires the
master branch of sparklyr (i.e., the version from
`devtools::install_github("sparklyr/sparklyr", ref = "master")` which is
newer that sparklyr 1.2) to be able to load the
`com.twosigma:sparklyr-flint_*` artifacts correctly from the
`https://dl.bintray.com/yl790/maven` repository, along with all
transitive dependencies from Maven central.

The `com.twosigma:sparklyr-flint_*` artifacts contain minor
modifications (mostly changes to the `build.sbt` file) needed to ensure
Flint time series functionalities work with Spark 2.4 and Spark 3.0.
Artifact names and locations are subject to change. They most likely
will be moved to Maven central in future, possibly under a different
group ID as well (TBD).

At the moment, Flint time series functionalities are accessible through
both Spark 2.x and Spark 3.0 via sparklyr, and some commonly used
summarizers such ‘count’ and ‘sum’ are working as expected through a
reasonably intuitive R interface (see example below). Meanwhile there
are still plenty of other Flint functionalities such as EWMA summarizer,
weighted mean, etc that will need similar R interfaces in
`sparklyr.flint`.

## Example Usage

First attach `sparklyr.flint` package and then connect to Spark from
sparklyr, e.g.,

``` r
library(sparklyr)
library(sparklyr.flint)

spark_version <- "2.4.0"
sc <- spark_connect(master = "local", version = spark_version)
```

or alternatively,

``` r
spark_version <- "3.0.0"
sc <- spark_connect(master = "local", version = spark_version)
```

since this extension also works with Spark 3.0.

For the purpose of this illustration, we shall create some simple data
points such that verifying the correctness of summarized results in all
examples below will be an easy exercise for the reader.

``` r
df <- tibble::tibble(
  t = c(1, 3, 4, 6, 7, 10, 15, 16, 18, 19),
  v = c(4, -2, NA, 5, NA, 1, -4, 5, NA, 3)
)
sdf <- copy_to(sc, df, overwrite = TRUE)
```

Next, we shall copy data points from above from a Spark data frame into
a `TimeSeriesRDD` so that Flint can analyze them:

``` r
ts_rdd <- fromSDF(sdf, is_sorted = TRUE, time_unit = "SECONDS", time_column = "t")
```

Alternatively, one can also create a builder object if the same
`(is_sorted, time_unit, time_column)` settings need to be applied to
multiple Spark data frames or RDDs:

``` r
builder <- ts_rdd_builder(sc, is_sorted = TRUE, time_unit = "SECONDS", time_column = "t")
ts_rdd_1 <- builder$fromSDF(sdf_1)
ts_rdd_2 <- builder$fromSDF(sdf_2)
ts_rdd_3 <- builder$fromRDD(rdd_3, schema_of_rdd_3)
```

Let’s say for each time point `t` (in seconds), we are interested in a
summary all rows within the time span of `[t - 3, t]`, then we can
specify the desired time window in R as `in_past("3s")`, and apply
various summarizers with this time window on the `TimeSeriesRDD` from
above.

``` r
ts_count <- summarize_count(ts_rdd, in_past("3s"))
ts_count %>% collect()
```

should output the total number of rows within each time window:

    ## # A tibble: 10 x 3
    ##    time                    v count
    ##    <dttm>              <dbl> <dbl>
    ##  1 1970-01-01 00:00:01     4     1
    ##  2 1970-01-01 00:00:03    -2     2
    ##  3 1970-01-01 00:00:04   NaN     3
    ##  4 1970-01-01 00:00:06     5     3
    ##  5 1970-01-01 00:00:07   NaN     3
    ##  6 1970-01-01 00:00:10     1     2
    ##  7 1970-01-01 00:00:15    -4     1
    ##  8 1970-01-01 00:00:16     5     2
    ##  9 1970-01-01 00:00:18   NaN     3
    ## 10 1970-01-01 00:00:19     3     3

``` r
ts_count <- summarize_count(ts_rdd, in_past("3s"), column = "v")
ts_count %>% collect()
```

should output the total number of values from column `v` that are not
`NULL` or `NaN` within each time window:

    ## # A tibble: 10 x 3
    ##    time                    v v_count
    ##    <dttm>              <dbl>   <dbl>
    ##  1 1970-01-01 00:00:01     4       1
    ##  2 1970-01-01 00:00:03    -2       2
    ##  3 1970-01-01 00:00:04   NaN       2
    ##  4 1970-01-01 00:00:06     5       2
    ##  5 1970-01-01 00:00:07   NaN       1
    ##  6 1970-01-01 00:00:10     1       1
    ##  7 1970-01-01 00:00:15    -4       1
    ##  8 1970-01-01 00:00:16     5       2
    ##  9 1970-01-01 00:00:18   NaN       2
    ## 10 1970-01-01 00:00:19     3       2

and

``` r
ts_sum <- summarize_sum(ts_rdd, in_past("3s"), "v")
ts_sum %>% collect()
```

should output the sum of values from column `v` within each time window,
ignoring `NULL` or `NaN` values:

    ## # A tibble: 10 x 3
    ##    time                    v v_sum
    ##    <dttm>              <dbl> <dbl>
    ##  1 1970-01-01 00:00:01     4     4
    ##  2 1970-01-01 00:00:03    -2     2
    ##  3 1970-01-01 00:00:04   NaN     2
    ##  4 1970-01-01 00:00:06     5     3
    ##  5 1970-01-01 00:00:07   NaN     5
    ##  6 1970-01-01 00:00:10     1     1
    ##  7 1970-01-01 00:00:15    -4    -4
    ##  8 1970-01-01 00:00:16     5     1
    ##  9 1970-01-01 00:00:18   NaN     1
    ## 10 1970-01-01 00:00:19     3     8
