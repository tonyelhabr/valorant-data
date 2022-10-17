
.compact <- function (l) Filter(Negate(is.null), l)

#' @importFrom cli cli_abort
.parse_repo <- function(repo){
  r <- strsplit(repo, "/")[[1]]
  
  if (length(r) != 2) {
    cli::cli_abort(
      c("Could not parse {.val {repo}} as a GitHub repository.",
        "Make sure you have used the format: {.val owner/repo}")
    )
  }
  
  return(r)
}

#' @importFrom gh gh gh_token
#' @importFrom cli cli_abort cli_warn
.pb_releases <- function(repo, .token = gh::gh_token(), verbose = getOption("piggyback.verbose", default = TRUE)) {
  
  r <- .parse_repo(repo)
  
  releases <- tryCatch(
    gh::gh(
      "/repos/:owner/:repo/releases",
      owner = r[[1]],
      repo = r[[2]],
      .limit = Inf,
      .token = .token
    ),
    error = function(cnd){
      
      cli::cli_abort(
        c("!"="Cannot access release data for repo {.val {repo}}.",
          "Check that you have provided a {.code .token} and that the repo is correctly specified.",
          unlist(strsplit(cnd$message, "\\n"))
        )
        
      )
    }
  )
  
  if(length(releases) == 0) {
    if(verbose){
      cli::cli_warn(
        c("!" = "No GitHub releases found for {.val {repo}}!",
          "You can make a new one with {.fun piggyback::pb_new_release}")
      )}
    return(invisible(data.frame()))
  }
  
  out <- data.frame(
    release_name = vapply(releases, `[[`, character(1),"name"),
    release_id = vapply(releases, `[[`, integer(1),"id"),
    release_body = vapply(releases, `[[`, character(1),"body"),
    tag_name = vapply(releases, `[[`, character(1),"tag_name"),
    draft = vapply(releases, `[[`, logical(1),"draft"),
    created_at = vapply(releases, `[[`, character(1),"created_at"),
    # published_at = vapply(releases, `[[`, character(1),"published_at"),
    html_url = vapply(releases, `[[`, character(1),"html_url"),
    upload_url = vapply(releases, `[[`,character(1),"upload_url"),
    n_assets = vapply(releases, function(x) length(x[["assets"]]), integer(1))
  )
  
  return(out)
}

#' @importFrom gh gh
#' @importFrom lubridate as_datetime
#' @noRd
.get_release_assets <- function(releases, r, .token) {
  
  if(nrow(releases)==0) return(data.frame())
  
  asset_list <- vector("list", length = nrow(releases))
  
  # fetch asset meta-data individually for each release, see #19
  for (i in seq_along(releases$tag_name)) {
    a <- gh::gh(endpoint = "/repos/:owner/:repo/releases/:release_id/assets",
                owner = r[[1]],
                repo = r[[2]],
                release_id = releases$release_id[[i]],
                .limit = Inf,
                .token = .token)
    if(length(a) == 0) next
    if (!identical(a[[1]], "")) {
      # convert list to dataframe and store in asset list
      a_df <- data.frame(
        file_name = vapply(a, `[[`, character(1), "name"),
        size = vapply(a, `[[`, integer(1), "size"),
        timestamp = lubridate::as_datetime(vapply(a, `[[`, character(1), "updated_at")),
        tag = releases$tag_name[i],
        owner = r[[1]],
        repo = r[[2]],
        upload_url = releases$upload_url[i],
        browser_download_url = vapply(a, `[[`, character(1L), "browser_download_url"),
        id = vapply(a, `[[`, integer(1L), "id"),
        state = vapply(a, `[[`, character(1L), "state"),
        stringsAsFactors = FALSE
      )
      
      asset_list[[i]] <- a_df
    }
  }
  
  # convert list of asset dataframes to single dataframe
  release_assets <- do.call(rbind,asset_list)
  
  # return result
  return(release_assets)
}

#' @importFrom gh gh_token
#' @importFrom lubridate as_datetime
.pb_info <- function(repo, tag = NULL, .token = gh::gh_token()) {
  
  r <- .parse_repo(repo)
  
  # get all releases
  releases <- .pb_releases(repo = repo, .token = .token, verbose = FALSE)
  
  # if no releases return empty df
  if(nrow(releases) == 0) {
    return(
      data.frame(
        file_name = "",
        size = 0L,
        timestamp = lubridate::as_datetime(0),
        tag = x$tag_name,
        owner = r[[1]],
        repo = r[[2]],
        upload_url = x$upload_url,
        browser_download_url = "",
        id = "",
        state = "",
        stringsAsFactors = FALSE
      ))
  }
  
  # if tag is latest, set tag to first tag present in releases
  if(length(tag)==1 && tag == "latest" && !"latest" %in% releases$tag_name) tag <- releases$tag_name[[1]]
  
  # if tag is present, filter the releases to search to just the tags requested
  if(!is.null(tag)) releases <- releases[releases$tag_name %in% tag,]
  
  # get release assets and metadata for each release
  info <- .get_release_assets(releases = releases, r = r, .token = .token)
  
  return(info)
}

#' @importFrom gh gh_token
#' @importFrom cli cli_alert_info cli_abort cli_alert_warning
.pb_upload <- function(
    file,
    repo,
    tag = "latest",
    name = NULL,
    overwrite = TRUE,
    show_progress = getOption("piggyback.verbose", default = interactive()),
    .token = gh::gh_token(),
    dir = NULL
) {
  
  stopifnot(
    is.character(repo),
    is.character(tag),
    length(tag) == 1,
    length(repo) == 1
  )
  
  releases <- .pb_releases(repo = repo, .token = .token, verbose = show_progress)
  
  if(tag == "latest" && length(releases$tag_name) > 0 && !"latest" %in% releases$tag_name) {
    if(getOption("piggyback.verbose", default = interactive())){
      cli::cli_alert_info("Uploading to latest release: {.val {releases$tag_name[[1]]}}.")
    }
    tag <- releases$tag_name[[1]]
  }
  
  if(!tag %in% releases$tag_name && !interactive()) {
    cli::cli_abort("Release {.val {tag}} not found in {.val {repo}}. No upload performed.")
  }
  
  out <- lapply(
    file, 
    function(f) {
      .pb_upload_file(
        f,
        repo = repo,
        tag = tag,
        name = name,
        overwrite = overwrite,
        show_progress = show_progress,
        .token = .token,
        dir = dir
      )
    }
  )
  
  invisible(out)
}

#' @importFrom cli cli_warn cli_alert_info
#' @importFrom httr progress RETRY add_headers upload_file warn_for_status
#' @importFrom fs file_info
#' @importFrom gh gh gh_token
.pb_upload_file <- function(
    file,
    repo = guess_repo(),
    tag = "latest",
    name = NULL,
    overwrite = TRUE,
    show_progress = getOption("piggyback.verbose", default = interactive()),
    .token = gh::gh_token(),
    dir = NULL
) {
  
  file_path <- do.call(file.path, .compact(list(dir,file)))
  
  if (!file.exists(file_path)) {
    cli::cli_warn("File {.file {file_path}} does not exist.")
    return(NULL)
  }

  progress <- httr::progress("up")
  if (!show_progress) progress <- NULL
  
  if (is.null(name)) {
    ## name is name on GitHub, technically need not be name of local file
    name <- basename(file_path)
  }
  
  ## memoised for piggyback_cache_duration
  df <- .pb_info(repo, tag, .token)
  
  i <- which(df$file_name == name)
  
  if (length(i) > 0) { # File of same name is on GitHub
    
    if (overwrite) {
      ## If we find matching id, Delete file from release.
      gh::gh(
        "DELETE /repos/:owner/:repo/releases/assets/:id",
        owner = df$owner[[1]],
        repo = df$repo[[1]],
        id = df$id[i],
        .token = .token
      )
    } else {
      cli::cli_warn("Skipping upload of {.file {df$file_name[i]}} as file exists on GitHub and {.code overwrite = FALSE}")
      return(invisible(NULL))
    }
  }
  
  if (show_progress) cli::cli_alert_info("Uploading {.file {name}} ...")
  
  releases <- .pb_releases(repo = repo, .token = .token, verbose = show_progress)
  upload_url <- releases$upload_url[releases$tag_name == tag]
  
  r <- httr::RETRY(
    verb = "POST",
    url = sub("\\{.+$", "", upload_url),
    query = list(name = name),
    httr::add_headers(Authorization = paste("token", .token)),
    body = httr::upload_file(file_path),
    progress,
    terminate_on = c(400, 401, 403, 404, 422)
  )
  
  cat("\n")
  
  if(show_progress) httr::warn_for_status(r)
  
  invisible(r)
}

#' @importFrom cli cli_abort cli_warn cli_alert_success
#' @importFrom httr RETRY add_headers http_error http_error content
#' @importFrom glue glue
#' @importFrom jsonlite toJSON
#' @importFrom gh gh_token
.pb_release_create <- function(
    repo,
    tag,
    commit = NULL,
    name = tag,
    body = "Data release",
    draft = FALSE,
    prerelease = FALSE,
    .token = gh::gh_token()
) {
  
  releases <- .pb_releases(repo = repo, .token = .token, verbose = FALSE)
  
  # if no releases exist, pb_releases returns a dataframe of releases
  if(nrow(releases) > 0 && tag %in% releases$tag_name){
    cli::cli_abort("Failed to create release: {.val {tag}} already exists!")
  }
  
  r <- .parse_repo(repo)
  
  payload <- .compact(list(
    tag_name = tag,
    target_commitish = commit,
    name = name,
    body = body,
    draft = draft,
    prerelease = prerelease
  ))
  
  ## gh fails to pass body correctly?
  # gh("/repos/:owner/:repo/releases", owner = r[[1]], repo = r[[2]],
  #  .method = "POST", body = toJSON(payload,auto_unbox = TRUE), encode="json")
  
  resp <- httr::RETRY(
    verb = "POST",
    url = glue::glue("https://api.github.com/repos/{r[[1]]}/{r[[2]]}/releases"),
    httr::add_headers(Authorization = paste("token",.token)),
    body = jsonlite::toJSON(payload, auto_unbox = TRUE),
    terminate_on = c(400, 401, 403, 404, 422)
  )
  
  if(httr::http_error(resp)) {
    cli::cli_warn(
      c("!"="Failed to create release: HTTP error {.val {httr::status_code(resp)}}.",
        "See returned error messages for more details"))
    
    return(httr::content(resp))
  }
  
  release <- httr::content(resp)
  cli::cli_alert_success("Created new release {.val {name}}.")
  invisible(release)
}

#' @importFrom cli cli_alert_success cli_abort cli_alert_success
#' @importFrom httr RETRY add_headers http_error
#' @importFrom glue glue
#' @importFrom gh gh_token
.pb_release_delete <- function(repo, tag, .token = gh::gh_token()) {
  
  releases <- .pb_releases(repo = repo, .token = .token)
  
  stopifnot(
    length(repo)   == 1 && is.character(repo),
    length(tag)    == 1 && is.character(tag),
    tag %in% releases$tag_name,
    length(.token) == 1 && is.character(.token) && nchar(.token) > 0
  )
  
  release_id <- releases$release_id[releases$tag_name == tag]
  r <- parse_repo(repo)
  
  resp <- httr::RETRY(
    verb = "DELETE",
    glue::glue(
      "https://api.github.com/repos/{owner}/{repo}/releases/{release_id}",
      owner = r[1],
      repo = r[2],
      release_id = release_id
    ),
    httr::add_headers(Authorization = paste("token",.token)),
    terminate_on = c(400, 401, 403, 404, 422)
  )
  
  if(httr::http_error(resp)){
    cli::cli_abort(
      c(
        "!" = "HTTP error {.val {httr::status_code(resp)}}:
        Could not delete release named {.val {tag}} in {.val {repo}}",
        "See returned error body for more details."
      )
    )
    return(resp)
  }
  resp2 <- httr::RETRY(
    verb = "DELETE",
    glue::glue(
      "https://api.github.com/repos/{owner}/{repo}/git/refs/tags/{tag}",
      owner = r[1],
      repo = r[2],
      tag = tag
    ),
    httr::add_headers(Authorization = paste("token",.token)),
    terminate_on = c(400, 401, 403, 404, 422)
  )
  
  if(httr::http_error(resp2)){
    cli::cli_abort(
      c(
        "!" = "HTTP error {.val {httr::status_code(resp2)}}:
        Could not delete git reference named {.val {tag}} in {.val {repo}}",
        "See returned error body for more details."
      )
    )
    return(resp2)
  }
  
  
  cli::cli_alert_success("Deleted release {.val {tag}} from {.val {repo}}.")
  
  invisible(resp)
}

#' @importFrom cli cli_warn cli_alert_info
#' @importFrom gh gh gh_token
.pb_delete <- function(file = NULL, repo, tag = "latest", .token = gh::gh_token()) {
  df <- .pb_info(repo, tag, .token)
  
  if (is.null(file)) ids <- df$id
  
  if(!is.null(file) && any(!file %in% df$file_name)){
    missing <- file[!file %in% df$file_name]
    file <- file[file != missing]
    cli::cli_warn("{.val {missing}} not found in {.val {tag}} release of {.val {repo}}")
  }
  
  if(!is.null(file)){
    ids <- df[df$file_name %in% file, "id"]
  }
  
  if (length(ids) < 1) {
    cli::cli_warn("No file deletions performed.")
    return(invisible(NULL))
  }
  
  lapply(ids, function(id) {
    ## If we find matching id, Delete file from release.
    gh::gh(
      "DELETE /repos/:owner/:repo/releases/assets/:id",
      owner = df$owner[[1]],
      repo = df$repo[[1]],
      id = id,
      .token = .token
    )
  })
  
  if(getOption("piggyback.verbose", default = TRUE)) cli::cli_alert_info("Deleted {.val {file}} from {.val {tag}} release on {.val {repo}}")
  
  return(invisible(TRUE))
}
