library(eurocontrol)
library(dplyr)
library(arrow)

# Fetch NM point profiles for flights in the APDF UGTB dataset
# NOTE: requires DB access (PRU_READ connection)

# Load APDF to get the NM FLIGHT_IDs (ID column)
apdf <- read_parquet("data/reference/apdf_UGTB_202603.parquet")

flight_ids <- apdf |>
  filter(!is.na(ID)) |>
  pull(ID) |>
  unique()

# Fetch CPF (Correlated Position reports) for March 2026
# Oracle IN clause limited to 1000 items — batch the flight IDs
id_batches <- split(flight_ids, ceiling(seq_along(flight_ids) / 999))

profiles <- purrr::map(id_batches, \(batch) {
  point_profiles_tidy(
    wef = "2026-03-01",
    til = "2026-04-01",
    profile = "CPF"
  ) |>
    filter(FLIGHT_ID %in% batch) |>
    collect()
}) |>
  bind_rows()

dir.create("data/reference", showWarnings = FALSE, recursive = TRUE)
write_parquet(profiles, "data/reference/cpf_profiles_UGTB_202603.parquet")

message(
  "Saved ", nrow(profiles), " CPF points for ",
  n_distinct(profiles$FLIGHT_ID), " flights."
)
