ml_print_uid <- function(x) cat(paste0("<", x$uid, ">"), "\n")

ml_print_class <- function(x, type) {
  type <- if (is_ml_estimator(x))
    "Estimator"
  else if (is_ml_transformer(x))
    "Transformer"
  else
    class(x)[1]
  cat(ml_short_type(x), paste0("(", type, ")\n"))
}

ml_print_input_output <- function(x) {
  out_names <- ml_param_map(x) %>%
    names() %>%
    (function(x) grep("col|cols$", x, value = TRUE))
  for (param in out_names)
    cat(paste0("  ", param, ": ", ml_param(x, param), "\n"))
}

ml_print_items <- function(x, items = NULL) {
  if (rlang::is_null(items))
    items <- names(x) %>%
      setdiff(c("uid", "param_map", "summary", ".jobj")) %>%
      (function(nms) grep(".*(?<!col|cols)$", nms, value = TRUE, perl = TRUE))
  for (item in items)
    if (!rlang::is_null(x[[item]]))
      cat(paste0("  ", item, ": ", capture.output(str(x[[item]]))), "\n")
}

ml_print_model <- function(x, items = NULL) {
  ml_print_class(x)
  ml_print_uid(x)
  ml_print_items(x, items)
  ml_print_input_output(x)
}

ml_print_model_summary <- function(x, items = NULL) {
  ml_print_class(x)
  items <- items %||% grep(".*(?<!col|cols)$", names(x), value = TRUE, perl = TRUE)
  ml_print_items(x, items)
  ml_print_input_output(x)
}

print_newline <- function() {
  cat("", sep = "\n")
}

ml_model_print_call <- function(model) {
  printf("Call: %s\n", model$.call)
  invisible(model$.call)
}

ml_model_print_residuals <- function(model,
                                     residuals.header = "Residuals") {

  residuals <- model$summary$residuals %>%
    (function(x) if (is.function(x)) x() else x) %>%
    spark_dataframe()

  # randomly sample residuals and produce quantiles based on
  # sample to avoid slowness in Spark's 'percentile_approx()'
  # implementation
  count <- invoke(residuals, "count")
  limit <- 1E5
  isApproximate <- count > limit
  column <- invoke(residuals, "columns")[[1]]

  values <- if (isApproximate) {
    fraction <- limit / count
    residuals %>%
      invoke("sample", FALSE, fraction) %>%
      sdf_read_column(column) %>%
      quantile()
  } else {
    residuals %>%
      sdf_read_column(column) %>%
      quantile()
  }
  names(values) <- c("Min", "1Q", "Median", "3Q", "Max")

  header <- if (isApproximate)
    paste(residuals.header, "(approximate):")
  else
    paste(residuals.header, ":", sep = "")

  cat(header, sep = "\n")
  print(values, digits = max(3L, getOption("digits") - 3L))
  invisible(values)
}

#' @importFrom stats coefficients quantile
ml_model_print_coefficients <- function(model) {

  coef <- coefficients(model)

  cat("Coefficients:", sep = "\n")
  print(coef)
  invisible(coef)
}

ml_model_print_coefficients_detailed <- function(model) {

  # extract relevant columns for stats::printCoefmat call
  # (protect against routines that don't provide standard
  # error estimates, etc)
  columns <- c("coefficients", "standard.errors", "t.values", "p.values")
  values <- as.list(model[columns])
  for (value in values)
    if (is.null(value))
      return(ml_model_print_coefficients(model))

  matrix <- do.call(base::cbind, values)
  colnames(matrix) <- c("Estimate", "Std. Error", "t value", "Pr(>|t|)")

  cat("Coefficients:", sep = "\n")
  stats::printCoefmat(matrix)
}

ml_model_print_centers <- function(model) {

  centers <- model$centers
  if (is.null(centers))
    return()

  cat("Cluster centers:", sep = "\n")
  print(model$centers)

}


#' Spark ML - Feature Importance for Tree Models
#'
#' @param sc A \code{spark_connection}.
#' @param model An \code{ml_model} encapsulating the output from a decision tree.
#'
#' @return A sorted data frame with feature labels and their relative importance.
#' @export
#' @importFrom dplyr arrange
#' @importFrom dplyr desc
ml_tree_feature_importance <- function(sc, model)
{
  supported <- c("ml_model_gradient_boosted_trees",
                 "ml_model_decision_tree",
                 "ml_model_random_forest")

  if (!inherits(model, supported)) {
    fmt <- "cannot call 'ml_tree_feature_importance' on object of class %s"
    deparsed <- paste(deparse(class(model), width.cutoff = 500), collapse = " ")
    stop(sprintf(fmt, deparsed))
  }

  # enforce Spark 2.0.0 for certain models
  requires_spark_2 <- c(
    "ml_model_decision_tree",
    "ml_model_gradient_boosted_trees"
  )

  if (inherits(model, requires_spark_2))
    spark_require_version(sc, "2.0.0")

  importance <- invoke(model$.model, "featureImportances") %>%
    invoke("toArray") %>%
    cbind(model$features) %>%
    as.data.frame()

  colnames(importance) <- c("importance", "feature")

  importance %>% arrange(desc(importance))
}