library(httr)
library(lubridate)
library(fs)

generate_urls <- function(data_type, start_date, end_date) {
  base_url <- paste0(
    "https://www.eurocontrol.int/performance/data/download/OPDI/v002/",
    data_type, "/", data_type, "_"
  )
  urls <- c()

  if (data_type == "flight_list") {
    start_dt <- ymd(paste0(start_date, "01"))
    end_dt <- ymd(paste0(end_date, "01"))
    delta <- months(1)
  } else {
    start_dt <- ymd(start_date)
    end_dt <- ymd(end_date)
    delta <- days(10)
  }

  current_dt <- start_dt
  while (current_dt <= end_dt) {
    if (data_type == "flight_list") {
      url <- paste0(base_url, format(current_dt, "%Y%m"), ".parquet")
    } else {
      next_dt <- current_dt + delta
      url <- paste0(
        base_url,
        format(current_dt, "%Y%m%d"), "_",
        format(next_dt, "%Y%m%d"), ".parquet"
      )
    }
    urls <- c(urls, url)
    current_dt <- current_dt + delta
  }

  return(urls)
}

download_files <- function(urls, save_folder) {
  if (!dir_exists(save_folder)) {
    dir_create(save_folder)
  }

  for (url in urls) {
    file_name <- basename(url)
    save_path <- file.path(save_folder, file_name)

    if (file_exists(save_path)) {
      message("Skipping ", file_name, ", already exists.")
      next
    }

    message("Downloading ", url, "...")

    tryCatch({
      response <- GET(url, write_disk(save_path, overwrite = TRUE))

      if (http_error(response)) {
        warning("Failed to download ", url, ": HTTP error ", status_code(response))
      } else {
        message("Saved to ", save_path)
      }
    }, error = function(e) {
      warning("Failed to download ", url, ": ", e$message)
    })
  }
}

# Download all data types covering March 2026
datasets <- list(
  "flight_list"   = c("202603", "202603"),
  "flight_events" = c("20260301", "20260331"),
  "measurements"  = c("20260301", "20260331")
)

for (data_type in names(datasets)) {
  dates <- datasets[[data_type]]
  urls <- generate_urls(data_type, dates[1], dates[2])
  download_files(urls, paste0("./data/", data_type))
}