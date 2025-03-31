#' Fetch and Combine WOAH Outbreak Data
#'
#' Fetches outbreak data (locations and full details) from the WOAH API for a
#' specified date range, potentially in batches, and returns the combined data
#' as a list.
#'
#' @details
#' This function performs the following steps:
#' \enumerate{
#'   \item Splits the date range into smaller intervals if necessary (using `batch_interval`).
#'   \item For each date interval:
#'     \itemize{
#'       \item Fetches location data using `rwahis::get_woah_outbreak_locations`.
#'       \item Fetches full outbreak details using `rwahis::get_woah_outbreaks_full_info`.
#'       \item Stores the fetched data in memory.
#'     }
#'   \item After fetching all data:
#'     \itemize{
#'       \item Combines the data from all successful batches.
#'       \item Returns the combined data.
#'     }
#' }
#' This function does *not* interact with any database.
#'
#' @param start_date Character string or Date object for the start of the overall date range (YYYY-MM-DD).
#' @param end_date Character string or Date object for the end of the overall date range (YYYY-MM-DD).
#' @param disease_name Optional: Character string specifying the exact disease name to filter by.
#'   If NULL (default), fetches data for all diseases.
#' @param batch_interval Character string specifying the interval for batch processing
#'   (e.g., "1 month", "2 weeks", "10 days"). Default is "1 month". Set to `NULL`
#'   to process the entire range at once (not recommended for large ranges).
#' @param language Language code for API requests (default: "en").
#' @param verbose Logical: Print progress messages? (Default: TRUE).
#'
#' @return A list containing two elements:
#'   \itemize{
#'     \item `locations`: A data frame of combined location data from all successful batches, or `NULL` if no location data was fetched.
#'     \item `details`: A list of data frames, where each element corresponds to a table from the full outbreak details (e.g., 'outbreak', 'adminDivisions', 'speciesQuantities'), combined across all successful batches. Returns `NULL` if no details data was fetched.
#'   }
#'   Returns `NULL` overall if a critical error occurs during input validation or date processing. Issues during API fetching for specific batches are reported as warnings, and data from failed batches is excluded.
#'
#' @importFrom lubridate ymd interval duration floor_date %m+% days weeks
#' @importFrom dplyr %>% filter select distinct anti_join mutate across all_of rename relocate if_else bind_rows summarise group_by n
#' @importFrom purrr map map_chr safely walk imap list_rbind set_names pluck keep compact map_lgl
#' @importFrom rlang := abort
#' @importFrom methods is
#' @importFrom utils head
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Fetch HPAI data for March 2025
#' hpai_data <- fetch_woah_data(
#'   start_date = "2025-03-01",
#'   end_date = "2025-03-31",
#'   disease_name = "Influenza A virus (Inf. with high pathogenicity) (OIE-listed)",
#'   batch_interval = "1 month"
#' )
#'
#' # Inspect the fetched data
#' if (!is.null(hpai_data)) {
#'   print(head(hpai_data$locations))
#'   print(names(hpai_data$details))
#'   if ("speciesQuantities" %in% names(hpai_data$details)) {
#'     print(head(hpai_data$details$speciesQuantities))
#'   }
#' }
#'
#' # Fetch all FMD data for Q1 2025 in 2-week batches
#' fmd_data <- fetch_woah_data(
#'   start_date = "2025-01-01",
#'   end_date = "2025-03-31",
#'   disease_name = "Foot and mouth disease virus (Inf. with) ",
#'   batch_interval = "2 weeks"
#' )
#' }
fetch_woah_data <- function(start_date,
                            end_date,
                            disease_name = NULL,
                            batch_interval = "1 month",
                            language = "en",
                            verbose = TRUE) {

  # --- Input Validation ---
  start_date <- tryCatch(lubridate::ymd(start_date), error = function(e) rlang::abort("Invalid start_date format. Use YYYY-MM-DD."))
  end_date <- tryCatch(lubridate::ymd(end_date), error = function(e) rlang::abort("Invalid end_date format. Use YYYY-MM-DD."))
  if (start_date > end_date) {
    rlang::abort("start_date cannot be after end_date.")
  }

  # --- Date Batching ---
  if (!is.null(batch_interval)) {
    interval_parts <- strsplit(batch_interval, " ")[[1]]
    if (length(interval_parts) != 2 || !grepl("^[1-9][0-9]*$", interval_parts[1]) || !interval_parts[2] %in% c("day", "days", "week", "weeks", "month", "months")) {
        warning("Invalid batch_interval format. Using '1 month'. Supported: N days/weeks/months (N=positive integer).")
        batch_interval <- "1 month"
        interval_parts <- c("1", "month")
    }
    num <- as.numeric(interval_parts[1])
    unit <- interval_parts[2]

    # Create sequence of start dates
    start_dates <- seq(start_date, end_date, by = batch_interval)
    # Create corresponding end dates
    end_dates_calc <- (start_dates +
                         if(grepl("month", unit)) months(num) else months(0) +
                         if(grepl("week", unit)) weeks(num) else weeks(0) +
                         if(grepl("day", unit)) days(num) else days(0)) - days(1)
    # Ensure last end date doesn't exceed overall end_date
    end_dates_calc[length(end_dates_calc)] <- min(end_dates_calc[length(end_dates_calc)], end_date)
    # Ensure start dates don't exceed end date and filter pairs
    valid_indices <- start_dates <= end_date
    start_dates <- start_dates[valid_indices]
    end_dates_calc <- end_dates_calc[valid_indices]
    # Ensure end date is not before start date
    valid_intervals <- start_dates <= end_dates_calc
    start_dates <- start_dates[valid_intervals]
    end_dates_final <- end_dates_calc[valid_intervals]


    if(length(start_dates) == 0) {
        message("No valid date intervals generated based on start/end dates and batch interval. Nothing to process.")
        return(list(locations = NULL, details = NULL)) # Return empty structure
    }

    date_intervals <- purrr::map2(start_dates, end_dates_final, ~list(start = .x, end = .y))
    if (verbose) message(sprintf("Processing date range in %d batches of interval '%s'.", length(date_intervals), batch_interval))
  } else {
    date_intervals <- list(list(start = start_date, end = end_date))
    if (verbose) message("Processing entire date range at once.")
  }

  # --- Data Accumulation ---
  all_locations_list <- list()
  all_details_list <- list()
  any_fetch_error <- FALSE

  if (verbose) message("\n--- Starting Data Fetching Phase ---")

  for (i in seq_along(date_intervals)) {
    interval <- date_intervals[[i]]
    int_start <- interval$start
    int_end <- interval$end
    if (verbose) message(sprintf("\n--- Fetching Batch %d/%d (%s to %s) ---", i, length(date_intervals), int_start, int_end))

    batch_locations <- NULL
    batch_details <- NULL
    fetch_error_current_batch <- FALSE

    tryCatch({
        if (verbose) {
            message(sprintf("Fetching locations for %s to %s...", int_start, int_end))
            if (!is.null(disease_name)) message(sprintf("  Disease filter: %s", disease_name))
        }
        # Use safely to capture errors without stopping the loop
        # Assuming get_woah_outbreak_locations is available in the environment or package namespace
        safe_get_locations <- purrr::safely(get_woah_outbreak_locations)
        locations_result <- safe_get_locations(
            start_date = int_start,
            end_date = int_end,
            disease_name = disease_name,
            language = language,
            verbose = verbose
        )

        if (!is.null(locations_result$error)) {
            warning(sprintf("Error fetching locations for interval %s - %s: %s. Skipping batch.", int_start, int_end, locations_result$error$message))
            fetch_error_current_batch <- TRUE
        } else if (is.null(locations_result$result) || nrow(locations_result$result) == 0) {
            if (verbose) message("No locations found for this interval.")
            batch_locations <- NULL
            batch_details <- NULL
        } else {
             batch_locations <- locations_result$result
             if (verbose) message(sprintf("Fetched %d location records.", nrow(batch_locations)))

             if (verbose) message("Fetching full outbreak details...")
             # Assuming get_woah_outbreaks_full_info is available
             safe_get_details <- purrr::safely(get_woah_outbreaks_full_info)
             details_result <- safe_get_details(
                 start_date = int_start,
                 end_date = int_end,
                 disease_name = disease_name,
                 language = language,
                 verbose = verbose
             )

             if (!is.null(details_result$error)) {
                 warning(sprintf("Error fetching full details for interval %s - %s: %s. Details for this batch might be incomplete.", int_start, int_end, details_result$error$message))
                 batch_details <- NULL
                 fetch_error_current_batch <- TRUE
             } else if (is.null(details_result$result) || length(details_result$result) == 0) {
                 if (verbose) message("No outbreak details found for this interval (despite locations existing).")
                 batch_details <- NULL
             } else {
                 batch_details <- details_result$result
                 if (verbose) {
                     total_records_in_details <- sum(sapply(batch_details, function(x) if(is.data.frame(x)) nrow(x) else 0))
                     message(sprintf("  Retrieved details with %d total records across all tables.", total_records_in_details))
                 }
             }
        }
    }, error = function(e) {
        warning(sprintf("Unexpected error during data fetching for interval %s - %s: %s. Skipping batch.", int_start, int_end, e$message))
        fetch_error_current_batch <- TRUE
    })

    # Store results if fetch was successful for the batch (or partially successful)
    if (!fetch_error_current_batch) {
        all_locations_list[[i]] <- batch_locations
        all_details_list[[i]] <- batch_details
    } else {
        all_locations_list[[i]] <- NULL
        all_details_list[[i]] <- NULL
        any_fetch_error <- TRUE
    }

  } # End loop through date_intervals

  if (verbose) message("\n--- Data Fetching Phase Complete ---")

  # --- Combine Data ---
  valid_locations <- purrr::compact(all_locations_list)
  valid_details <- purrr::compact(all_details_list)

  if (length(valid_locations) == 0 && length(valid_details) == 0) {
      message("No data successfully fetched from any batch.")
      return(list(locations = NULL, details = NULL))
  }

  if (verbose) message("Combining fetched data...")

  # Combine locations
  combined_locations_data <- NULL
  if (length(valid_locations) > 0) {
      combined_locations_data <- tryCatch({
          dplyr::bind_rows(valid_locations)
      }, error = function(e) {
          warning("Error combining location data frames: ", e$message)
          NULL
      })
      if (!is.null(combined_locations_data) && verbose) {
          message(sprintf("Combined %d location records from %d batches.", nrow(combined_locations_data), length(valid_locations)))
      }
       # Rename columns after combining, before returning
       if (!is.null(combined_locations_data)) {
           cols_to_rename_loc <- intersect(names(combined_locations_data), c("outbreakId", "reportId", "eventId", "isLocationApprox", "startDate", "endDate", "totalCases", "totalOutbreaks", "oieReference", "nationalReference", "disease"))
           if(length(cols_to_rename_loc) > 0) {
               combined_locations_data <- combined_locations_data %>%
                   rename(
                       outbreak_id = outbreakId, report_id = reportId, event_id = eventId,
                       is_location_approx = isLocationApprox, start_date = startDate, end_date = endDate,
                       total_cases = totalCases, total_outbreaks = totalOutbreaks,
                       oie_reference = oieReference, national_reference = nationalReference,
                       disease_name = disease
                   )
           }
       }
  } else {
      if (verbose) message("No valid location data fetched.")
  }


  # Combine details
  combined_details_list <- NULL
  if (length(valid_details) > 0) {
      detail_names <- names(valid_details[[1]]) # Assumes first is representative
      combined_details_list <- tryCatch({
          purrr::map(detail_names, function(name) {
              batch_dfs <- purrr::map(valid_details, ~ purrr::pluck(.x, name)) %>% purrr::compact()
              if (length(batch_dfs) > 0) {
                  if(all(purrr::map_lgl(batch_dfs, is.data.frame))) {
                      dplyr::bind_rows(batch_dfs)
                  } else {
                      warning(sprintf("Non-data frame element found when combining details for '%s'. Skipping.", name))
                      NULL
                  }
              } else {
                  NULL
              }
          }) %>% purrr::set_names(detail_names) %>%
          purrr::compact()
      }, error = function(e) {
          warning("Error combining details lists: ", e$message)
          NULL
      })
       if (!is.null(combined_details_list) && verbose) {
           total_combined_details <- sum(sapply(combined_details_list, function(x) if(is.data.frame(x)) nrow(x) else 0))
           message(sprintf("Combined details lists from %d batches, resulting in %d total records across %d tables.",
                           length(valid_details), total_combined_details, length(combined_details_list)))
       }
        # Rename columns within the combined details list
       if (!is.null(combined_details_list)) {
            combined_details_list <- purrr::imap(combined_details_list, function(data, element_name) {
                current_data_renamed <- data
                if (element_name == "outbreak") {
                    cols_to_rename <- intersect(names(current_data_renamed), c("outbreakId", "reportId", "areaId", "isLocationApprox", "clusterCount", "startDate", "endDate", "createdByReportId", "lastUpdateReportId", "epiUnitType_id", "epiUnitType_keyValue", "epiUnitType_translation", "description_original", "description_translation"))
                    if(length(cols_to_rename) > 0) current_data_renamed <- current_data_renamed %>% rename(outbreak_id = outbreakId, report_id = reportId, area_id = areaId, is_location_approx = isLocationApprox, cluster_count = clusterCount, start_date = startDate, end_date = endDate, created_by_report_id = createdByReportId, last_update_report_id = lastUpdateReportId, epi_unit_type_id = epiUnitType_id, epi_unit_type_key_value = epiUnitType_keyValue, epi_unit_type_translation = epiUnitType_translation, description_original = description_original, description_translation = description_translation)
                } else if (element_name == "quantityUnit") {
                    cols_to_rename <- intersect(names(current_data_renamed), c("outbreakId", "reportId", "keyValue"))
                    if(length(cols_to_rename) > 0) current_data_renamed <- current_data_renamed %>% rename(outbreak_id = outbreakId, report_id = reportId, key_value = keyValue)
                } else if (element_name == "adminDivisions") {
                    cols_to_rename <- intersect(names(current_data_renamed), c("outbreakId", "reportId", "areaId", "adminLevel", "parentAreaId"))
                    if(length(cols_to_rename) > 0) current_data_renamed <- current_data_renamed %>% rename(outbreak_id = outbreakId, report_id = reportId, area_id = areaId, admin_level = adminLevel, parent_area_id = parentAreaId)
                } else if (element_name == "speciesQuantities") {
                    cols_to_rename <- intersect(names(current_data_renamed), c("outbreakId", "reportId", "speciesId", "speciesName", "isWild", "createdByCurrentReport"))
                    if(length(cols_to_rename) > 0) current_data_renamed <- current_data_renamed %>% rename(outbreak_id = outbreakId, report_id = reportId, species_id = speciesId, species_name = speciesName, is_wild = isWild, created_by_current_report = createdByCurrentReport)
                } else if (element_name == "diagnosticMethods") {
                    cols_to_rename <- intersect(names(current_data_renamed), c("outbreakId", "reportId", "nature_id", "nature_keyValue", "nature_translation", "nature_description"))
                    if(length(cols_to_rename) > 0) current_data_renamed <- current_data_renamed %>% rename(outbreak_id = outbreakId, report_id = reportId, nature_id = nature_id, nature_key_value = nature_keyValue, nature_translation = nature_translation, nature_description = nature_description)
                }
                # Add other renames as needed
                return(current_data_renamed)
            })
       }

  } else {
       if (verbose) message("No valid details data fetched.")
  }

  # --- Final Message & Return ---
  if (verbose) message("\n--- Data Fetching Function Finished ---")
  if (any_fetch_error) {
    message("Fetching completed with errors for one or more batches. Returned data may be incomplete.")
  } else {
    if (verbose) message("Fetching completed successfully.")
  }

  return(list(locations = combined_locations_data, details = combined_details_list))

}
