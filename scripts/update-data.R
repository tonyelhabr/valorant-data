pkgload::load_all()

now <- as.POSIXlt(Sys.time(), tz = 'UTC')
releases <- get_valorant_releases()

events <- update_events(timestamp = now, releases = releases)
series <- update_series(events, timestamp = now, releases = releases)
matches <- update_matches(series, timestamp = now, releases = releases)
match_details <- update_match_details(matches, timestamp = now, releases = releases)
players <- update_players(matches, timestamp = now, releases = releases)
