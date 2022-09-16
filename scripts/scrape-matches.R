library(valorantr)
library(dplyr)
library(purrr)
library(lubridate)
library(qs)

data_dir <- 'data'
series_dir <- file.path('data', 'series')
matches_dir <- file.path('data', 'matches')
match_details_dir <- file.path('data', 'match-details')
c(data_dir, series_dir, matches_dir, match_details_dir) |> 
  walk(dir.create, showWarnings = FALSE)

get_and_save_f <- function(id, f, dir, overwrite = FALSE, sleep = runif(1, min = 0, max = 1)) {
  
  path <- file.path(dir, sprintf('%s.qs', id))
  if (file.exists(path) & !overwrite) {
    return(qread(path))
  }
  Sys.sleep(sleep)
  message(sprintf('Retrieving data for %s.', id))
  res <- f(id)
  qsave(res, path)
  res
}

get_and_save_series <- partial(
  get_and_save_f,
  f = get_series,
  dir = series_dir,
  ... =
)

get_and_save_matches <- partial(
  get_and_save_f,
  f = get_matches,
  dir = matches_dir,
  ... =
)

get_and_save_match_details <- partial(
  get_and_save_f,
  f = get_match_details,
  dir = match_details_dir,
  ... =
)

event_regexpr <- 'VALORANT Champions'

now <- lubridate::now(tzone = 'UTC')

events <- get_events(event_regexpr) |>
  filter(grepl(event_regexpr, name))

in_process_events <- events |> 
  transmute(id, end_time = ymd_hms(endDate)) |> 
  filter(end_time > !!now)

events_have_been_updated <- nrow(in_process_events) > 0

if (isTRUE(events_have_been_updated)) {
  attr(events, 'timestamp') <- now
  qsave(events, file.path(data_dir, 'events.qs'))
}


if (isTRUE(events_have_been_updated)) {
  series <- events$id |> map_dfr(get_and_save_series)
  
  series_path <- file.path(data_dir, 'series.qs')
  new_matches_possibly_exist <- TRUE
  if (isTRUE(file.exists(series_path))) {
    old_series <- qread(series_path)
    diff_series_id <- setdiff(series$id, old_series$id)
    if (length(diff_series_id) == 0) {
      new_matches_possibly_exist <- FALSE
    }
  } else {
    attr(series, 'timestamp') <- now
    qsave(series, series_path)
  }
  
  if (isTRUE(new_matches_possibly_exist)) {
    matches <- series$id |>
      unique() |> 
      map(get_and_save_matches)
    
    matches_path <- file.path(data_dir, 'matches.qs')
    has_new_matches <- TRUE
    if (isTRUE(file.exists(matches_path))) {
      old_matches <- qread(matches_path)
      diff_matches_id <- setdiff(matches$id, old_matches$id)
      if (length(diff_matches_id) == 0) {
        has_new_matches <- FALSE
      }
    } else {
      attr(matches, 'timestamp') <- now
      qsave(matches, matches_path)
    }
    
    
    if (isTRUE(has_new_matches)) {
      match_details <- matches |> 
        map(~pluck(.x, 'matches', 'id')) |> 
        flatten_int() |> 
        unique() |> 
        map(get_and_save_match_details)
      
      attr(match_details, 'timestamp') <- now
      qsave(match_details, file.path(data_dir, 'match_details.qs'))
    }
  }
}
