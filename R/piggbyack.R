
#' @importFrom curl curl_fetch_memory
#' @importFrom qs qdeserialize
#' @source <https://github.com/nflverse/nflreadr/blob/main/R/from_url.R#L185>
#' @noRd
qs_from_url <- function(url) {
  load <- curl::curl_fetch_memory(url)
  qs::qdeserialize(load$content)
}

# valorant_download <- function(tag, dir = tempdir(check = TRUE), ...) {
#   file <- sprintf('%s.qs', tag)
#   res <- pb_download(
#     file,
#     repo = valorant_repo,
#     tag = tag,
#     ...
#   )
#   file.path(dir, file)
# }
# 
# valorant_read <- function(tag, ...) {
#   path <- valorant_download(tag = tag, ...)
#   qread(path)
# }

valorant_read <- function(tag) {
  url <- sprintf('https://github.com/%s/releases/download/%s/%s.qs', valorant_repo, tag, tag)
  qs_from_url(url)
}

#' @importFrom piggyback pb_upload
#' @noRd
valorant_upload <- function(file, tag, ...) {
  piggyback::pb_upload(
    file,
    repo = valorant_repo,
    tag = tag,
    ...
  )
}

## Note that this is really only used for the README. The timestamp is also logged as an attribute to the release data.
#' @importFrom jsonlite toJSON
#' @source <https://github.com/nflverse/nflverse-data/R/upload.R#L16>
#' @noRd
update_release_timestamp <- function(temp_dir, tag = tag, timestamp, ...){
  path <- file.path(temp_dir, 'timestamp.json')
  
  update_time <- format(timestamp, tz = "UTC", usetz = TRUE)
  
  list(last_updated = update_time) |>
    jsonlite::toJSON(auto_unbox = TRUE) |>
    writeLines(path)
  
  valorant_upload(file = path, tag = tag, overwrite = TRUE, ...)
}


#' @importFrom qs qsave
#' @noRd
valorant_write <- function(df, tag = deparse(substitute(df)), timestamp, ...) {
  temp_dir <- tempdir(check = TRUE)
  path <- file.path(temp_dir, sprintf('%s.qs', tag))
  
  attr(df, 'timestamp') <- timestamp
  qs::qsave(df, path)
  valorant_upload(file = path, tag = tag, ...)
  
  update_release_timestamp(temp_dir = temp_dir, tag = tag, timestamp = timestamp)
  
  invisible(df)
}

#' @importFrom piggyback pb_new_release
#' @noRd
valorant_new_release <- function(tag, ...) {
  piggyback::pb_new_release(
    repo = valorant_repo,
    tag = tag,
    ...
  )
}

#' @importFrom piggyback pb_releases
#' @noRd
valorant_releases <- function() {
  piggyback::pb_releases(repo = valorant_repo)
}

#' @importFrom piggyback pb_releases
#' @noRd
valorant_release_exists <- function(tag, releases = NULL) {
  if (is.null(releases)) {
    releases <- valorant_releases()
  }
  any(tag == releases$release_name)
}

