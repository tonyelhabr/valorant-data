
#' @importFrom dplyr transmute filter
get_in_progress_events <- function(events, timestamp) {
  events |> 
    dplyr::transmute(.data$id, endDate = strptime(events$endDate, '%Y-%m-%dT%H:%M:%S.000Z', tz = 'UTC')) |> 
    dplyr::filter(.data$endDate > timestamp)
}

#' @importFrom valorantr get_events load_valorant
#' @importFrom purrr map_dfr
#' @importFrom dplyr filter bind_rows distinct arrange
#' @importFrom rlang .data
#' @noRd
update_events <- function(events, timestamp, releases = NULL, overwrite = getOption('valorant.data.overwrite', default = FALSE)) {
  
  release_exists <- valorant_release_exists(tag = 'events', releases = releases)
  
  if (isTRUE(overwrite)) {
    f <- if (isTRUE(release_exists)) {
      possibly_delete_and_rerelase_valorant
    } else {
      release_create_valorant
    }
    f(
      tag = 'events',
      body = 'Professional Valorant events'
    )
  }
  
  old_events <- if (isTRUE(release_exists)) {
    valorantr::load_valorant('events')
  } else {
    data.frame()
  }
  
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
    message(
      paste0('Updating the following events:\n', paste0(new_events$id, collapse = '\n'))
    )
    events <- dplyr::bind_rows(
      new_events,
      events |> dplyr::filter(!(.data$id %in% new_events$id))
    )
  }
  
  events <- save_valorant(events, tag = 'events', timestamp = timestamp)
  events
}

#' @importFrom purrr map_dfr
#' @importFrom valorantr get_series load_valorant
#' @importFrom dplyr transmute filter
#' @importFrom rlang .data
#' @importFrom tibble tibble
#' @noRd
update_series <- function(events, timestamp, releases = NULL, overwrite = getOption('valorant.data.overwrite', default = FALSE)) {
  
  release_exists <- valorant_release_exists(tag = 'series', releases = releases)
  id_release_exists <- valorant_release_exists(tag = 'series_event_ids', releases = releases)
  
  if (isTRUE(overwrite)) {
    
    f <- if (isTRUE(release_exists)) {
      possibly_delete_and_rerelase_valorant
    } else {
      release_create_valorant
    }
    f(
      tag = 'series', 
      body = 'Professional Valorant series'
    )
    
    f <- if (isTRUE(id_release_exists)) {
      possibly_delete_and_rerelase_valorant
    } else {
      release_create_valorant
    }
    f(
      tag = 'series_event_ids', 
      body = 'Professional Valorant events for which series have been scraped'
    )
  }
  
  if (isTRUE(overwrite)) {
    series <- events$id |> purrr::map_dfr(valorantr::get_series)
    event_ids <- tibble::tibble(event_id = unique(series$eventId))
  } else {
    old_series <- valorantr::load_valorant('series')
    old_event_ids <- valorantr::load_valorant('series_event_ids')
    
    in_progress_events <- get_in_progress_events(events, timestamp = timestamp)
    events_are_in_progress <- nrow(in_progress_events) > 0
    
    new_event_ids <- unique(setdiff(events$id, old_event_ids$event_id))
    new_events_have_occurred <- length(new_event_ids) > 0
    
    if (isFALSE(events_are_in_progress) & isFALSE(new_events_have_occurred)) {
      message('Returning early because there are no in progress events and no new events.')
      return(old_series)
    } else {
      
      if (isTRUE(events_are_in_progress)) {
        in_progress_series <- in_progress_events$id |> purrr::map_dfr(valorantr::get_series)
        in_progress_series_ids <- setdiff(in_progress_series$id, old_series$id)
        
        in_progress_series_have_occurred <- length(in_progress_series_ids) > 0
        
        if (isFALSE(new_events_have_occurred) & isFALSE(in_progress_series_have_occurred)) {
          return(old_series)
        }
        
      } else {
        in_progress_series <- tibble::tibble()
        in_progress_series_ids <- integer()
      }
      
      if (isTRUE(new_events_have_occurred)) {
        new_series <- new_event_ids |> purrr::map_dfr(valorantr::get_series)
        new_series_ids <- setdiff(new_series$id, old_series$id)
        
        new_series_have_occurred <- length(new_series_ids) > 0
        
        if (isFALSE(events_are_in_progress) & isFALSE(new_series_have_occurred)) {
          return(old_series)
        }
        
      } else {
        new_series <- tibble::tibble()
        new_series_ids <- integer()
      }
      
      new_series_ids <- unique(c(in_progress_series_ids, new_series_ids))
      
      if (length(new_series_ids) == 0) {
        message('Returning early because there are no in progress events and no new events.')
        return(old_series)
      }
      
      message(
        paste0('Updating the following series:\n', paste0(new_series_ids, collapse = '\n'))
      )
      
      new_series <- dplyr::bind_rows(
        in_progress_series,
        new_series
      ) |> 
        dplyr::filter(.data$id %in% new_series_ids)
      series <- dplyr::bind_rows(new_series, old_series)
      event_ids <- tibble::tibble(event_id = unique(series$eventId))
      
    }
  }
  
  series <- save_valorant(series, tag = 'series', timestamp = timestamp)
  series_event_ids <- save_valorant(event_ids, tag = 'series_event_ids', timestamp = timestamp)
  series
}

#' @importFrom valorantr get_matches load_valorant
#' @importFrom purrr map pluck flatten_int possibly
#' @noRd
update_matches <- function(series, timestamp, releases = NULL, overwrite = getOption('valorant.data.overwrite', default = FALSE)) {
  
  release_exists <- valorant_release_exists(tag = 'matches', releases = releases)
  
  if (isTRUE(overwrite)) {
    
    f <- if (isTRUE(release_exists)) {
      possibly_delete_and_rerelase_valorant
    } else {
      release_create_valorant
    }
    
    f(
      tag = 'matches', 
      body = 'Professional Valorant matches'
    )
    
  }
  
  possibly_get_matches <- purrr::possibly(
    valorantr::get_matches, 
    otherwise = list(), 
    quiet = FALSE
  )
  
  if (isTRUE(overwrite)) {
    matches <- series$id |> unique() |> purrr::map(possibly_get_matches)
  } else {
    old_matches <- valorantr::load_valorant('matches')
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
      
      message(
        paste0('Updating the matches for the following series:\n', paste0(new_series_ids, collapse = '\n'))
      )
      
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
update_match_details <- function(matches, timestamp, releases = NULL, overwrite = getOption('valorant.data.overwrite', default = FALSE)) {
  
  release_exists <- valorant_release_exists(tag = 'match_details', releases = releases)
  
  if (isTRUE(overwrite)) {
    
    f <- if (isTRUE(release_exists)) {
      possibly_delete_and_rerelase_valorant
    } else {
      release_create_valorant
    }
    
    f(
      tag = 'match_details', 
      body = 'Professional Valorant match details'
    )
  }
  
  match_ids <- matches |> 
    purrr::map(~purrr::pluck(.x, 'matches', 'id')) |> 
    purrr::flatten_int() |> 
    unique()
  
  possibly_get_match_details <- purrr::possibly(
    valorantr::get_match_details, 
    otherwise = list(), 
    quiet = FALSE
  )
  
  if (isTRUE(overwrite)) {
    match_details <- match_ids |> purrr::map(possibly_get_match_details)
  } else {
    old_match_details <- valorantr::load_valorant('match_details')
    old_match_ids <- old_match_details |> 
      purrr::map_int(~purrr::pluck(.x, 'id')) |> 
      unique()
    
    new_match_ids <- setdiff(match_ids, old_match_ids)
    new_matches_have_occurred <- length(new_match_ids) > 0
    
    if (isFALSE(new_matches_have_occurred)) {
      return(old_match_details)
    } else {
      
      message(
        paste0('Updating the details for the following matches:\n', paste0(new_match_ids, collapse = '\n'))
      )
      
      new_match_details <- new_match_ids |> purrr::map(possibly_get_match_details)
      match_details <- append(new_match_details, old_match_details)
      
    }
  }
  match_details <- save_valorant(match_details, tag = 'match_details', timestamp = timestamp)
  match_details
}

#' @importFrom purrr map pluck flatten_int
pluck_match_ids <- function(matches) {
  matches |> 
    purrr::map(~purrr::pluck(.x, 'matches', 'id')) |> 
    purrr::flatten_int() |> 
    unique() |> 
    sort(decreasing = TRUE)
}

#' @importFrom valorantr get_player load_valorant
#' @importFrom purrr map_dfr pluck map keep
#' @importFrom dplyr distinct mutate filter arrange bind_rows
#' @importFrom tibble tibble
#' @noRd
update_players <- function(matches, timestamp, releases = NULL, overwrite = getOption('valorant.data.overwrite', default = FALSE)) {
  
  release_exists <- valorant_release_exists(tag = 'players', releases = releases)
  id_release_exists <- valorant_release_exists(tag = 'player_match_ids', releases = releases)
  
  if (isTRUE(overwrite)) {
    
    f <- if (isTRUE(release_exists)) {
      possibly_delete_and_rerelase_valorant
    } else {
      release_create_valorant
    }
    f(
      tag = 'players', 
      body = 'Professional Valorant players'
    )
    
    f <- if (isTRUE(id_release_exists)) {
      possibly_delete_and_rerelase_valorant
    } else {
      release_create_valorant
    }
    f(
      tag = 'player_match_ids', 
      body = 'Match ids from which players have been retrieved'
    )
    
    release_exists <- FALSE
  }
  
  if (isTRUE(overwrite) | (isFALSE(release_exists) | isFALSE(id_release_exists))) {
    players <- matches |> 
      purrr::map_dfr(~purrr::pluck(.x, 'playerStats')) |> 
      dplyr::distinct(player_id = .data$playerId) |> 
      dplyr::arrange(.data$player_id) |> 
      dplyr::mutate(
        data = purrr::map(.data$player_id, get_player)
      )
    
    player_match_ids <- tibble::tibble(match_id = player_match_ids)
  }
  
  if (isTRUE(release_exists)) {
    old_players <- valorantr::load_valorant('players')
    old_player_match_ids <- valorantr::load_valorant('player_match_ids') |> 
      purrr::pluck('match_id')
  } else {
    old_players <- data.frame()
    old_player_match_ids <- integer()
  }
  
  player_match_ids <- matches |> pluck_match_ids()
  
  new_match_ids <- setdiff(player_match_ids, old_player_match_ids)
  new_matches_have_occurred <- length(new_match_ids) > 0
  
  if (isFALSE(new_matches_have_occurred)) {
    return(old_players)
  } else {
    
    player_match_ids <- tibble::tibble(
      match_id = c(
        new_match_ids, 
        old_player_match_ids
      ) |> 
        unique() |> 
        sort(decreasing = TRUE)
    )
    
    match_ids_in_new_match_ids <- function(x) {
      any(purrr::pluck(x, 'matches', 'id') %in% new_match_ids)
    }
    
    init_new_players <- matches |> 
      purrr::keep(match_ids_in_new_match_ids) |> 
      purrr::map_dfr(~purrr::pluck(.x, 'playerStats')) |> 
      dplyr::distinct(player_id = .data$playerId) |> 
      dplyr::arrange(.data$player_id)
    
    new_player_ids <- setdiff(init_new_players$player_id, old_players$player_id)
    new_players_exist <- length(new_player_ids) > 0
    
    if (isFALSE(new_players_exist)) {
      player_match_ids <- save_valorant(player_match_ids, tag = 'player_match_ids', timestamp = timestamp)
      return(old_players)
    }
    
    new_players <- init_new_players |> 
      dplyr::filter(.data$player_id %in% new_player_ids) |> 
      dplyr::mutate(
        data = purrr::map(player_id, get_player)
      )
    
    players <- dplyr::bind_rows(
      new_players,
      old_players
    )
    
  }
  
  players <- save_valorant(players, tag = 'players', timestamp = timestamp)
  player_match_ids <- save_valorant(player_match_ids, tag = 'player_match_ids', timestamp = timestamp)
  players
}


