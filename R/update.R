
#' @importFrom dplyr transmute filter
get_in_progress_events <- function(events, timestamp) {
  events |> 
    dplyr::transmute(.data$id, endDate = strptime(events$endDate, '%Y-%m-%dT%H:%M:%S.000Z', tz = 'UTC')) |> 
    dplyr::filter(.data$endDate > timestamp)
}

#' @importFrom valorantr get_events load_valorant
#' @importFrom dplyr filter bind_rows
#' @importFrom rlang .data
#' @noRd
update_events <- function(event_regexpr = 'VALORANT Champions', ..., timestamp, releases = NULL) {
  
  release_exists <- valorant_release_exists(tag = 'events', releases = releases)
  
  if (isTRUE(release_exists)) {
    old_events <- valorantr::load_valorant('events')
  }
  
  events <- valorantr::get_events(event_regexpr, ...) |>
    dplyr::filter(grepl(event_regexpr, .data$name))
  
  if (isFALSE(release_exists)) {
    release_new_valorant(tag = 'events', body = 'Professional Valorant events')
  } else {
    
    completely_new_event_ids <- setdiff(events$id, old_events$id)
    completely_new_events_have_been_identified <- length(completely_new_event_ids) > 0
    
    completely_new_events <- if (isTRUE(completely_new_events_have_been_identified)) {
      events |> dplyr::filter(.data$id %in% completely_new_event_ids)
    } else {
      data.frame()
    }
    
    shared_event_ids <- intersect(events$id, old_events$id)
    shared_events_have_been_identified <- length(shared_event_ids) > 0
    
    shared_and_in_progress_events <-if (isTRUE(shared_events_have_been_identified)) {
      
       in_progress_events <- events |> 
        dplyr::filter(.data$id %in% shared_event_ids) |> 
        get_in_progress_events(timestamp = timestamp)
       
       events |> 
         dplyr::filter(.data$id %in% in_progress_events$id)
       
    } else {
      data.frame()
    }
    
    new_events <- dplyr::bind_rows(completely_new_events, shared_and_in_progress_events)
    events_have_been_updated <- nrow(new_events) > 0
    
    if (isFALSE(events_have_been_updated)) {
      return(old_events)
    } else {
      events <- dplyr::bind_rows(
        new_events,
        events |> dplyr::filter(!(.data$id %in% new_events$id))
      )
    }
  }
  
  events <- save_valorant(events, tag = 'events', timestamp = timestamp)
  events
}

#' @importFrom purrr map_dfr
#' @importFrom valorantr get_series load_valorant
#' @importFrom dplyr transmute filter
#' @importFrom rlang .data
#' @noRd
update_series <- function(events, timestamp, releases = NULL, overwrite = getOption('valorant.data.ovewrite', default = FALSE)) {
  
  release_exists <- valorant_release_exists(tag = 'series', releases = releases)
  
  if (isTRUE(overwrite) | isFALSE(release_exists)) {
    series <- events$id |> purrr::map_dfr(valorantr::get_series)
  }

  if (isTRUE(release_exists)) {
    old_series <- valorantr::load_valorant('series')
  }

  if (isFALSE(release_exists)) {
    release_new_valorant(tag = 'series', body = 'Professional Valorant series')
  } else {
    
    in_progress_events <- get_in_progress_events(events, timestamp = timestamp)
    events_are_in_progress <- nrow(in_progress_events) > 0
    
    if (isFALSE(events_are_in_progress)) {
      return(old_series)
    } else {
      
      new_series <- in_progress_events$id |> purrr::map_dfr(valorantr::get_series)
      new_series_ids <- setdiff(new_series$id, old_series$id)
      
      new_series_have_occurred <- length(new_series_ids) > 0
      
      if (isFALSE(new_series_have_occurred)) {
        return(old_series)
      }
      
      new_series <- new_series |> dplyr::filter(.data$id %in% new_series_ids)
      series <- dplyr::bind_rows(new_series, old_series)
      
    }
    
  }
  
  series <- save_valorant(series, tag = 'series', timestamp = timestamp)
  series
}

#' @importFrom valorantr get_matches load_valorant
#' @importFrom purrr map pluck flatten_int
#' @noRd
update_matches <- function(series, timestamp, releases = NULL, overwrite = getOption('valorant.data.ovewrite', default = FALSE)) {
  
  release_exists <- valorant_release_exists(tag = 'matches', releases = releases)
  
  if (isTRUE(overwrite) | isFALSE(release_exists)) {
    matches <- series$id |> unique() |> purrr::map(valorantr::get_matches)
  }
  
  if (isTRUE(release_exists)) {
    old_matches <- valorantr::load_valorant('matches')
  }
  
  if (isFALSE(release_exists)) {
    release_new_valorant(tag = 'matches', body = 'Professional Valorant matches')
  } else {
    
    old_series_ids <- old_matches |> 
      purrr::map(~purrr::pluck(.x, 'matches', 'seriesId')) |> 
      purrr::flatten_int() |> 
      unique()
    
    series_ids <- unique(series$id)
    
    new_series_ids <- setdiff(series_ids, old_series_ids)
    new_series_have_occurred <- length(new_series_ids) > 0
    
    if (isFALSE(new_series_have_occurred)) {
      return(old_matches)
    } else {
      new_matches <- new_series_ids |> purrr::map(valorantr::get_matches)
      matches <- append(new_matches, old_matches)
    }
    
  }
  
  matches <- save_valorant(matches, tag = 'matches', timestamp = timestamp)
  matches
}

#' @importFrom valorantr get_match_details load_valorant
#' @importFrom purrr map pluck flatten_int map_int
#' @noRd
update_match_details <- function(matches, timestamp, releases = NULL, overwrite = getOption('valorant.data.ovewrite', default = FALSE)) {
  
  release_exists <- valorant_release_exists(tag = 'match_details', releases = releases)
  
  match_ids <- matches |> 
    purrr::map(~purrr::pluck(.x, 'matches', 'id')) |> 
    purrr::flatten_int() |> 
    unique()
  
  if (isTRUE(overwrite) | isFALSE(release_exists)) {
    match_details <- match_ids |> purrr::map(valorantr::get_match_details)
  }
  
  if (isTRUE(release_exists)) {
    old_match_details <- valorantr::load_valorant('match_details')
  }
  
  if (isFALSE(release_exists)) {
    release_new_valorant(tag = 'match_details', body = 'Professional Valorant match details')
  } else {

    old_match_ids <- old_match_details |> 
      purrr::map_int(~purrr::pluck(.x, 'id')) |> 
      unique()

    new_match_ids <- setdiff(match_ids, old_match_ids)
    new_matches_have_occurred <- length(new_match_ids) > 0
    
    if (isFALSE(new_matches_have_occurred)) {
      return(old_match_details)
    } else {
      
      new_match_details <- new_match_ids |> purrr::map(valorantr::get_match_details)
      match_details <- append(new_match_details, old_match_details)
      
    }
    
  }
  
  match_details <- save_valorant(match_details, tag = 'match_details', timestamp = timestamp)
  match_details
}
