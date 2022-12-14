
#' @importFrom piggyback pb_upload
#' @noRd
upload_valorant <- function(file, tag, overwrite = TRUE, ...) {
  # piggyback::pb_upload(
  .pb_upload(
    file,
    repo = valorant_repo,
    tag = tag,
    overwrite = overwrite,
    ...
  )
}

## Note that this is really only used for the README. The timestamp is also logged as an attribute to the release data.
#' @importFrom jsonlite toJSON
#' @source <https://github.com/nflverse/nflverse-data/R/upload.R#L16>
#' @noRd
update_release_timestamp <- function(temp_dir, tag = tag, timestamp, ...){
  path <- file.path(temp_dir, 'timestamp.json')
  
  update_time <- format(timestamp, tz = 'UTC', usetz = TRUE)
  
  list(last_updated = update_time) |>
    jsonlite::toJSON(auto_unbox = TRUE) |>
    writeLines(path)
  
  upload_valorant(file = path, tag = tag, ...)
}


#' @importFrom qs qsave
#' @noRd
save_valorant <- function(df, tag = deparse(substitute(df)), timestamp, ...) {
  temp_dir <- tempdir(check = TRUE)
  path <- file.path(temp_dir, sprintf('%s.qs', tag))
  
  attr(df, 'timestamp') <- timestamp
  qs::qsave(df, path)
  upload_valorant(file = path, tag = tag, ...)
  
  update_release_timestamp(temp_dir = temp_dir, tag = tag, timestamp = timestamp)
  
  invisible(df)
}

#' @importFrom piggyback pb_new_release
#' @noRd
release_create_valorant <- function(tag, ...) {
  # piggyback::pb_release_create(
  .pb_release_create(
    repo = valorant_repo,
    tag = tag,
    .token = .token,
    ...
  )
}

#' @importFrom piggyback pb_releases
#' @noRd
get_valorant_releases <- function(...) {
  # piggyback::pb_releases(
  .pb_releases(
    repo = valorant_repo, 
    ...
  )
}

#' @importFrom piggyback pb_releases
#' @noRd
valorant_release_exists <- function(tag, ..., releases = NULL) {
  if (is.null(releases)) {
    releases <- get_valorant_releases(...)
  }
  any(tag == releases$release_name)
}

#' @importFrom piggyback pb_delete pb_release_delete
#' @noRd
delete_valorant <- function(tag, ...) {
  # piggyback::pb_delete(
  .pb_delete(
    repo = valorant_repo,
    tag = tag,
    ...
  )
  # piggyback::pb_release_delete(
  .pb_release_delete(
    repo = valorant_repo,
    tag = tag,
    ...
  )
}
possibly_release_create_valorant <- purrr::possibly(release_create_valorant, otherwise = NULL)
possibly_delete_valorant <- purrr::possibly(delete_valorant, otherwise = NULL)

possibly_delete_and_rerelase_valorant <- function(tag, body, ...) {
  possibly_delete_valorant(
    tag = tag,
    ...
  )
  
  possibly_release_create_valorant(
    tag = tag,
    body = body,
    ...
  )
}
