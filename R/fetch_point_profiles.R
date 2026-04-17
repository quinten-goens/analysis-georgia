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
# point_profiles_tidy returns a lazy dbplyr query — filter before collecting
profiles <- point_profiles_tidy(
  wef = "2026-03-01",
  til = "2026-04-01",
  profile = "CPF"
) |>
  filter(FLIGHT_ID %in% flight_ids) |>
  collect()

dir.create("data/reference", showWarnings = FALSE, recursive = TRUE)
write_parquet(profiles, "data/reference/cpf_profiles_UGTB_202603.parquet")

message(
  "Saved ", nrow(profiles), " CPF points for ",
  n_distinct(profiles$FLIGHT_ID), " flights."
)
