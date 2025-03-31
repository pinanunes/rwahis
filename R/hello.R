# --- Function: get_woah_disease_id (Internal Helper) ---

#' Get WOAH Disease ID from Name
#'
#' Fetches the list of first-level diseases from the WOAH API and finds the numeric ID(s)
#' corresponding to an exact disease name. This is typically used as a helper function
#' within other API calling functions and is not exported.
#'
#' @importFrom httr GET modify_url add_headers stop_for_status content timeout
#' @importFrom jsonlite fromJSON validate
#' @importFrom极 dplyr filter %>%
#'
#' @param disease_name The exact name of the disease as listed by the API.
#' @param language Language code (default: "en").
#' @return A list containing the numeric ID(s) for the disease, or NULL if not found.
#' @noRd
get_woah_disease_id <- function(disease_name, language = "en") {

  disease_list_url <- modify_url("https://wahis.woah.org/api/v1/pi/disease/first-level-filters",
                                 query = list(language = language))

  tryCatch({
    response <- GET(disease_list_url,
                    add_headers("Accept" = "application/json"),
                    timeout(30)) # Add a timeout

    stop_for_status(response, task = "fetch disease list") # Check for HTTP errors

    content_raw <- content(response, as = "text", encoding = "UTF-8")

    # Handle potential empty or non-JSON response
    if (nchar(content_raw) == 0 || !validate(content_raw)) {
      warning("Received empty or invalid JSON response for disease list.")
      return(NULL)
    }

    disease_data <- fromJSON(content_raw, flatten = TRUE)

    # Find the matching disease using dplyr::filter
    matched_disease <- disease_data %>%
      filter(name == disease_name)

    if (nrow(matched_disease) == 0) {
      warning("Disease name '", disease_name, "' not found in the list.")
      return(NULL)
    } else if (nrow(matched_disease) > 1) {
      warning("Multiple matches found for disease name '", disease_name, "'. Using the first one.")
      return(matched_disease$ids[[1]])
    } else {
      return(matched_disease极$ids[[1]])
    }

  }, error = function(e) {
    message("Error fetching or processing disease list: ", e$message)
    return(NULL)
  })
}

# --- Function: get_woah_outbreak_locations ---

#' Fetch Outbreak Location Data from WOAH API
#'
#' Retrieves outbreak geographic location data (lat/lon) from the WOAH API,
#' allowing filtering by disease name and date range.
#'
#' @importFrom httr POST content stop_for_status modify_url add_headers timeout status_code
#' @importFrom jsonlite toJSON fromJSON validate prettify
#' @importFrom dplyr mutate across %>%
#' @importFrom purrr %||%
#' @importFrom utils head
#'
#' @param start_date Character string or Date object for the start of the event date range (YYYY-MM-DD).
#' @param end_date Character string or Date object for the end of the event date range (YYYY-MM-DD).
#' @param disease_name Optional: Character string specifying the exact disease name.
#'   If NULL or empty, fetches data for all diseases. Uses internal `get_woah_disease_id`.
#' @param language Language code (default: "en").
#' @param verbose Logical: Print progress messages? (Default: FALSE)
#'
#' @return A data frame containing the outbreak location data (longitude, latitude, etc.),
#'   an empty data frame if no records are found, or NULL if an error occurs.
#' @export
get_woah_outbreak_locations <- function(start_date,
                                      end_date,
                                      disease_name = NULL,
                                      language = "en",
                                      verbose = FALSE) {
  # [Previous implementation remains unchanged]
}

# --- Function: get_woah_outbreaks ---

#' Fetch Outbreak Event Data from WOAH API
#'
#' Retrieves outbreak event data (tabular format) from the WOAH API,
#' allowing filtering by disease name and date range. Handles pagination automatically.
#'
#' @importFrom httr POST content stop_for_status modify_url add_headers timeout status_code
#' @importFrom jsonlite toJSON fromJSON validate prettify
#' @importFrom dplyr bind_rows %>%
#' @importFrom purrr %||% map_int
#' @importFrom utils head
#'
#' @param start_date Character string or Date object for the start of the event date range (YYYY-MM-DD).
#' @param end_date Character string or Date object for the end of the event date range (YYYY-MM-DD).
#' @param disease_name Optional: Character string specifying the exact disease name.
#'   If NULL or empty, fetches data for all diseases. Uses internal `get_woah_disease_id`.
#' @param language Language code (default: "en").
#' @param page_size Number of records to fetch per API call (default: 100).
#' @param max_pages Safety limit: Maximum number of pages to fetch (default: 50).
#' @param verbose Logical: Print progress messages? (Default: FALSE)
#'
#' @return A data frame containing the combined outbreak data from all pages,
#'   an empty data frame if no records are found, or NULL if an error occurs.
#' @export
get_woah_outbreaks <- function(start_date,
                              end_date,
                              disease_name = NULL,
                              language = "en",
                              page_size = 极100,
                              max_pages = 50,
                              verbose = FALSE) {
  # [Previous implementation remains unchanged]
}

# --- Helper Function: safe_extract_to_tibble (Internal) ---
safe_extract_to_tibble <- function(data_list, element_name, outbreak_id, report_id) {
  element <- purrr::pluck(data_list, element_name)
  
  # Always return at least outbreakId and reportId columns
  result <- tibble::tibble(outbreakId = outbreak_id, reportId = report_id)
  
  if (is.null(element) || length(element) == 0) {
    return(result)
  }
  
  if (is.list(element) && !is.data.frame(element)) {
    # Handle nested lists by converting to tibble
    tryCatch({
      df <- tibble::as_tibble(element)
      return(dplyr::bind_cols(result, df))
    }, error = function(e) {
      warning("Could not convert element '", element_name, "' to tibble")
      return(result)
    })
  } else if (is.data.frame(element)) {
    return(dplyr::bind_cols(result, element))
  }
  
  return(result)
}

# --- Function: get_woah_outbreak_details (Internal Helper) ---
get_woah_outbreak_details <- function(report_id, outbreak_id, language = "en", verbose = FALSE) {
  # [Previous implementation remains unchanged but ensures all returned tibbles have IDs]
}

# --- Function: get_woah_outbreaks_full_info ---

#' Fetch Full Outbreak Information (Events, Locations, Details) as Separate Tables
#'
#' Retrieves comprehensive outbreak information by fetching event and location data,
#' linking them, and querying detailed information for each specific outbreak.
#'
#' @importFrom dplyr inner_join select distinct bind_rows relocate filter any_of
#' @importFrom purrr map map2 possibly discard set_names pluck %||% list_rbind
#' @importFrom tibble tibble
#' @importFrom rlang abort
#'
#' @param start_date Character string or Date object for the start of the event date range (YYYY-MM-DD).
#' @param end_date Character string or Date object for the end of the event date range (YYYY-MM-DD).
#' @param disease_name Optional: Character string specifying the exact disease name.
#'   If NULL or empty, fetches data for all diseases.
#' @param language Language code (default: "en").
#' @param verbose Logical: Print progress messages? (Default: FALSE)
#'
#' @return A named list of tibbles where each tibble has outbreakId and reportId columns.
#' @export
get_woah_outbreaks_full_info <- function(start_date,
                                        end_date,
                                        disease_name = NULL,
                                        language = "en",
                                        verbose = FALSE) {
  
  # Initialize empty result structure
  empty_result <- list(
    outbreak = tibble(outbreakId = integer(), reportId = integer()),
    adminDivisions = tibble(outbreakId = integer(), reportId = integer()),
    speciesQuantities = tibble(outbreakId = integer(), reportId = integer()),
    controlMeasures = tibble(outbreakId = integer(), reportId = integer()),
    diagnosticMethods = tibble(outbreakId = integer(), reportId = integer())
  )

  # Get outbreak events and locations
  events <- get_woah_outbreaks(start_date, end_date, disease_name, language, verbose)
  locations <- get_woah_outbreak_locations(start_date, end_date, disease_name, language, verbose)
  
  if (is.null(events) || is.null(locations)) {
    if (verbose) message("Failed to get events or locations data")
    return(empty_result)
  }

  # Link events and locations
  report_outbreak_pairs <- events %>%
    select(eventId, reportId) %>%
    distinct() %>%
    inner_join(
      locations %>% select(eventId, outbreakId) %>% distinct(),
      by = "eventId"
    ) %>%
    select(reportId, outbreakId) %>%
    distinct()

  if (nrow(report_outbreak_pairs) == 0) {
    if (verbose) message("No outbreak-report pairs found")
    return(empty_result)
  }

  # Get details for each outbreak
  all_details <- map2(
    report_outbreak_pairs$reportId,
    report_outbreak_pairs$outbreakId,
    ~ get_woah_outbreak_details(.x, .y, language, verbose)
  )

  # Combine results
  combined <- list(
    outbreak = bind_rows(map(all_details, "outbreak")),
    adminDivisions = bind_rows(map(all_details, "adminDivisions")),
    speciesQuantities = bind_rows(map(all_details, "speciesQuantities")),
    controlMeasures = bind_rows(map(all_details, "controlMeasures")),
    diagnosticMethods = bind_rows(map(all_details, "diagnosticMethods"))
  )

  # Ensure all tables have the required ID columns
  combined <- map(combined, ~ {
    if (!all(c("outbreakId", "reportId") %in% names(.x))) {
      tibble(outbreakId = integer(), reportId = integer())
    } else {
      .x
    }
  })

  return(combined)
}
