library(arrow)
library(dplyr)

# --- 1. Filter flights with ADEP or ADES = "UGTB" (Tbilisi) ----------------
flights <- open_dataset("data/flight_list/") |>
  filter(adep == "UGTB" | ades == "UGTB") |>
  collect()

flight_ids <- flights$id

# --- 2. Filter flight events to matching flight IDs -------------------------
events <- open_dataset("data/flight_events/") |>
  filter(flight_id %in% flight_ids) |>
  collect()

event_ids <- events$id

# --- 3. Filter measurements to matching event IDs ---------------------------
measurements <- open_dataset("data/measurements/") |>
  filter(event_id %in% event_ids) |>
  collect()

# --- 4. Write filtered data --------------------------------------------------
dir.create("data/filtered", showWarnings = FALSE, recursive = TRUE)

write_parquet(flights,     "data/filtered/flights_UGTB.parquet")
write_parquet(events,      "data/filtered/events_UGTB.parquet")
write_parquet(measurements, "data/filtered/measurements_UGTB.parquet")
