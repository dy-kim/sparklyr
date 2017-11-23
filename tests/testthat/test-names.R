context("names")
test_requires("dplyr")
sc <- testthat_spark_connection()

df <- data_frame(col0 = 1:3,
                 col1 = letters[1:3],
                 col2 = letters[4:6])

tbl_spark_name <- "df_tbl_spark"
df_tbl_spark <- sdf_copy_to(
  sc = sc,
  x = df,
  name = tbl_spark_name,
  memory = TRUE,
  repartition = 0L,
  ovewrite = TRUE
)

# sdf helper function: sdf_table_name -------------------------------------------------

test_that("'sdf_table_name' returns the table name in Spark",
          {
            expect_equal(sdf_table_name(df_tbl_spark), tbl_spark_name)
          })

test_that("'sdf_table_name' stops if input is not an object of 'tbl_spark'",
          {
            expect_false("tbl_spark" %in% class(df))
            expect_error(sdf_table_name(df))
          })

# names ------------------------------------------------------------------------------

test_that("'names' returns header as expected", {
  expect_equal(names(df_tbl_spark), names(df))
  expect_equal(names(df_tbl_spark),
               df_tbl_spark %>% collect() %>% names())

})

test_that("'names' assigns new header as expected", {
  df_tbl_spark_new_header <- df_tbl_spark
  new_header <- c("a", "b", "c")
  names(df_tbl_spark_new_header) <- new_header

  expect_equal(names(df_tbl_spark_new_header), new_header)
  expect_equal(names(df_tbl_spark_new_header),
               df_tbl_spark_new_header %>% collect() %>% names())
  expect_error(names(df_tbl_spark_new_header) <- c(new_header, "d"))
})

test_that("'setNames' assigns new header as expected", {
  df_tbl_spark_new_header <- df_tbl_spark
  new_header <- c("a", "b", "c")
  df_tbl_spark_new_header <- setNames(df_tbl_spark_new_header, new_header)

  expect_equal(names(df_tbl_spark_new_header), new_header)
  expect_equal(names(df_tbl_spark_new_header),
               df_tbl_spark_new_header %>% collect() %>% names())
  expect_error(names(df_tbl_spark_new_header) <- c(new_header, "d"))
})
