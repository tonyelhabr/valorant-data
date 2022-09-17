
#' @importFrom valorantr get_events
#' @importFrom dplyr filter all_equal
#' @importFrom piggyback pb_releases
update_events <- function(event_regexpr = 'VALORANT Champions', timestamp, releases = NULL) {
  
  events <- valorantr::get_events(event_regexpr) |>
    dplyr::filter(grepl(event_regexpr, .data$name))
  
  release_exists <- valorant_release_exists(tag = 'events', releases = releases)
  if (isFALSE(release_exists)) {
    valorant_new_release(tag = 'events')
  } else {
    old_events <- valorant_read('events')
    events_have_been_updated <- dplyr::all_equal(
      events,
      old_events
    )
    if (isFALSE(events_have_been_updated)) {
      return(old_events)
    }
  }
  
  events <- valorant_write(events, tag = 'events', timestamp = timestamp)
  events
}

#' @importFrom purrr map_dfr
#' @importFrom valorantr get_series
#' @importFrom dplyr transmute filter
#' @importFrom lubridate ymd_hms
update_series <- function(events, timestamp, releases = NULL, overwrite = FALSE) {
  
  release_exists <- valorant_release_exists(tag = 'series', releases = releases)
  
  if (isTRUE(overwrite) | isFALSE(release_exists)) {
    series <- events$id |> purrr::map_dfr(valorantr::get_series)
  }

  if (isTRUE(release_exists)) {
    old_series <- valorant_read('series')
  }

  if (isFALSE(release_exists)) {
    
    valorant_new_release(tag = 'series')
    
  } else {
    
    in_progress_events <- events |> 
      dplyr::transmute(.data$id, end_time = lubridate::ymd_hms(.data$endDate)) |> 
      dplyr::filter(.data$end_time > timestamp)
    
    events_are_in_progress <- nrow(in_progress_events) > 0
    
    if (isFALSE(events_are_in_progress)) {
      
      return(old_series)
      
    } else {
      
      new_series <- in_progress_events$id |> purrr::map_dfr(valorantr::get_series)
      new_series_ids <- setdiff(series$id, old_series$id)
      new_series <- new_series |> dplyr::filter(.data$id %in% new_series_ids)
      series <- dplyr::bind_rows(new_series, old_series)
      
    }
    
  }
  
  series <- valorant_write(series, tag = 'series', timestamp = timestamp)
  series
}

#' Update matches
#' 
#' This will not work properly for in-progress series since it checks for the completion of series.
#' 
#' @importFrom valorantr get_matches
#' @importFrom purrr map pluck flatten_int
update_matches <- function(series, timestamp, releases = NULL) {
  
  release_exists <- valorant_release_exists(tag = 'matches', releases = releases)
  
  if (isTRUE(overwrite) | isFALSE(release_exists)) {
    matches <- series$id |> unique() |> purrr::map(valorantr::get_matches)
  }
  
  if (isTRUE(release_exists)) {
    old_matches <- valorant_read('matches')
  }
  
  series_timestamp <- attr(series, 'timestamp')
  if (isFALSE(release_exists)) {
    
    valorant_new_release(tag = 'matches')
    
  } else {
    
    # series_have_been_updated <- timestamp <= series_timestamp
    old_match_ids <- old_matches |> 
      purrr::map(~purrr::pluck(.x, 'matches', 'id')) |> 
      purrr::flatten_int() |> 
      unique()
    
    match_ids <- series |> 
      purrr::pluck('matches') |> 
      purrr::map(~purrr::pluck(.x, 'id')) |>
      purrr::flatten_int() |> 
      unique()
    
    new_match_ids <- setdiff(match_ids, old_match_ids)
    new_matches_hav_occurred <- length(new_match_ids) > 0
    
    if (isFALSE(new_match_ids)) {
      
      return(old_matches)
      
    } else {
 
      new_matches <- new_match_ids |> purrr::map(valorantr::get_matches)
      matches <- dplyr::bind_rows(new_matches, old_matches)
      
    }
    
  }
  
  matches <- valorant_write(matches, tag = 'matches', timestamp = timestamp)
  matches
}

#' @importFrom purrr map pluck flatten_int
update_match_details <- function(matches, timestamp, releases = NULL) {
  match_ids <- matches |> 
    purrr::map(~purrr::pluck(.x, 'matches', 'id')) |> 
    purrr::flatten_int() |> 
    unique()
  match_details <- match_ids |> purrr::map(get_match_details)
}
