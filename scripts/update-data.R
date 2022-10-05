pkgload::load_all()

# options('valorant.data.overwrite' = TRUE)

now <- as.POSIXlt(Sys.time(), tz = 'UTC')
releases <- get_valorant_releases()

init_events <- valorantr::get_events(n_results = 2000) |> tibble::as_tibble()

filter_events <- function(region_id, continent_regexpr) {
  init_events |> 
    dplyr::filter(.data$regionId == as.integer(region_id)) |> 
    dplyr::filter(
      .data$name |> stringr::str_detect(sprintf('^(VCT|Valorant).*(%s)', continent_regexpr))
    ) |> 
    dplyr::filter(
      .data$name |> stringr::str_detect('Game Changers', negate = TRUE)
    )
}

events <- bind_rows(
  init_events |> filter(regionId == 7),
  c(
    'EMEA|Europe|CIS|East Asia|Turkey',
    'North America|NA',
    'APAC|Southeast Asia|Japan|Korea',
    'South America|LATAM|BR|Brazil',
    'Arab',
    'Oceana'
  ) |> 
    rlang::set_names(as.character(1:6)) |> 
    purrr::imap_dfr(
      ~filter_events(region_id = .y, .x)
    )
) |> 
  dplyr::distinct(.data$id, .keep_all = TRUE) |> 
  dplyr::filter(
    .data$shortName |> stringr::str_detect('Open', negate = TRUE)
  ) |> 
  dplyr::filter(
    !is.na(.data$childLabel)
  ) |> 
  dplyr::arrange(dplyr::desc(.data$endDate))

events <- update_events(events, timestamp = now, releases = releases)
series <- update_series(events, timestamp = now, releases = releases)
matches <- update_matches(series, timestamp = now, releases = releases)
match_details <- update_match_details(matches, timestamp = now, releases = releases)
players <- update_players(matches, timestamp = now, releases = releases)
