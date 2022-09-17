.onLoad <- function(libname, pkgname) {
  if (is.null(getOption("valorant.data.overwrite"))) {
    options("valorant.data.overwrite" = FALSE)
  }
}
