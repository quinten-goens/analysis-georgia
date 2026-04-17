library(eurocontrol)
library(dplyr)
library(arrow)

# Fetch APDF data for UGTB (Tbilisi) — March 2026
# NOTE: requires DB access (PRU_ATMAP connection)

apdf <- apdf_tidy(wef = "2026-03-01", til = "2026-04-01") |>
  filter(ADEP_ICAO == "UGTB" | ADES_ICAO == "UGTB") |>
  collect()

dir.create("data/reference", showWarnings = FALSE, recursive = TRUE)
write_parquet(apdf, "data/reference/apdf_UGTB_202603.parquet")
