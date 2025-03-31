# --- Function: get_woah_disease_id (Internal Helper) ---

#' Get WOAH Disease ID from Name
#'
#' Fetches the list of first-level diseases from the WOAH API and finds the numeric ID(s)
#' corresponding to an exact disease name. This is typically used as a helper function
#' within other API calling functions and is not exported.
#'
#' @importFrom httr GET modify_url add_headers stop_for_status content timeout
#' @importFrom jsonlite fromJSON validate
#' @importFrom dplyr filter %>%
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
      # Return the 'ids' which is already a list in the JSON structure
      return(matched_disease$ids[[1]])
    } else {
      # Return the 'ids' which is already a list in the JSON structure
      return(matched_disease$ids[[1]])
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
#' @examples
#' \dontrun{
#'   # Get locations for Foot and Mouth disease outbreaks in March 2025
#'   fmd_locations <- get_woah_outbreak_locations(
#'     start_date = "2025-03-01",
#'     end_date = "2025-03-26",
#'     disease_name = "Foot and mouth disease virus (Inf. with) "
#'   )
#'   if (!is.null(fmd_locations)) print(utils::head(fmd_locations))
#'
#'   # Get locations for all outbreaks in the first week of March 2025
#'   all_locations <- get_woah_outbreak_locations(
#'     start_date = "2025-03-01",
#'     end_date = "2025-03-07",
#'     disease_name = NULL # Or omit the parameter
#'   )
#'   if (!is.null(all_locations)) print(utils::head(all_locations))
#' }
#' @export
get_woah_outbreak_locations <- function(start_date,
                                        end_date,
                                        disease_name = NULL,
                                        language = "en",
                                        verbose = FALSE) {

  # --- Input Validation ---
  tryCatch({
    start_date_fmt <- format(as.Date(start_date), "%Y-%m-%d")
    end_date_fmt <- format(as.Date(end_date), "%Y-%m-%d")
    if (start_date_fmt > end_date_fmt) {
      stop("start_date cannot be after end_date")
    }
  }, error = function(e) {
    stop("Invalid date format or range. Please use YYYY-MM-DD. Error: ", e$message)
  })

  # --- Get Disease ID (using internal helper) ---
  disease_ids <- list() # Use an empty list for "all diseases"
  if (!is.null(disease_name) && nzchar(disease_name)) {
    if (verbose) message("Looking up ID for disease: ", disease_name)
    # Call the internal helper function directly
    disease_ids_result <- get_woah_disease_id(disease_name, language)
    if (is.null(disease_ids_result)) {
      warning("Could not find ID for disease: ", disease_name, ". Fetching for all diseases instead.")
      # Keep disease_ids as an empty list
    } else {
      disease_ids <- disease_ids_result
      if (verbose) message("Found ID(s): ", paste(disease_ids, collapse = ", "))
    }
  } else {
    if (verbose) message("Fetching location data for all diseases.")
  }


  # --- API Request Setup ---
  api_url <- modify_url("https://wahis.woah.org/api/v1/pi/map-data/outbreaks-from-event-filter",
                        query = list(language = language))

  if (verbose) message("Preparing location data request...")

  # --- Construct Request Body as an R List ---
  request_body_list <- list(
    eventIds = list(),
    reportIds = list(),
    countries = list(),
    firstDiseases = if (length(disease_ids) > 0) I(disease_ids) else list(), # Keep the I() fix
    secondDiseases = list(),
    typeStatuses = list(),
    reasons = list(),
    eventStatuses = list(),
    reportTypes = list(),
    reportStatuses = list(),
    eventStartDate = list(from = start_date_fmt, to = end_date_fmt),
    submissionDate = NULL,
    animalTypes = list(),
    sortColumn = "submissionDate",
    sortOrder = "desc"
  )

  # --- Explicitly Encode to JSON ---
  json_payload_string <- toJSON(request_body_list, auto_unbox = TRUE, force = TRUE, null = "null")

  if (verbose) {
    message("---- Sending JSON Payload for Locations ----")
    # Removed message(prettify(json_payload_string))
    message("------------------------------------------")
  }

  # --- Make API Call ---
  location_data <- tryCatch({
    response <- POST(
      url = api_url,
      body = json_payload_string,
      add_headers(
        "Content-Type" = "application/json",
        "Accept" = "application/json"
      ),
      timeout(90)
    )

    stop_for_status(response, task = "fetch outbreak locations")

    content_raw <- content(response, as = "text", encoding = "UTF-8")

    if (nchar(content_raw) == 0 || !validate(content_raw)) {
      warning("Received empty or invalid JSON response for locations.")
      return(NULL) # Return NULL on invalid JSON
    }

    # Directly parse the JSON array
    parsed_data <- fromJSON(content_raw, flatten = TRUE)

    # Check if the result is a data frame or can be coerced
    if (is.data.frame(parsed_data) && nrow(parsed_data) > 0) {
      if (verbose) message(sprintf("Successfully received %d location records.", nrow(parsed_data)))

      # --- Data Type Conversion (Optional but Recommended) ---
      # Ensure lat/lon are numeric, dates are dates
      # Use dplyr::mutate and dplyr::across
      parsed_data <- parsed_data %>%
        mutate(
          across(any_of(c("longitude", "latitude", "totalCases", "totalOutbreaks")), .fns = as.numeric), # Use any_of for safety
          across(any_of(c("startDate", "endDate")), .fns = ~ suppressWarnings(as.Date(.x))) # Use as.Date for YYYY-MM-DD
        )

      return(parsed_data)

    } else if (is.list(parsed_data) && length(parsed_data) == 0) {
      if (verbose) message("No location records found matching the criteria.")
      return(data.frame()) # Return empty data frame for consistency
    } else {
      warning("Parsed location data is not in the expected data frame format.")
      return(NULL) # Return NULL if format is unexpected
    }

  }, error = function(e) {
    message("\nError during API call for outbreak locations.")
    # Attempt to get status code and response body even on error
    status_code_val <- tryCatch(status_code(response), error = function(e2) NA)
    response_content_on_error <- tryCatch(content(response, as="text", encoding="UTF-8"), error = function(e2) "")

    message("Status Code: ", status_code_val %||% "N/A") # Use purrr::%||%
    if (nzchar(response_content_on_error)) {
      message("Response Content on Error:\n", response_content_on_error)
    }
    message("Original R Error: ", e$message)

    # Return NULL on error
    return(NULL)
  }) # End tryCatch

  return(location_data)
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
#' @examples
#' \dontrun{
#'   # Get Foot and Mouth disease outbreaks in March 2025
#'   fmd_data <- get_woah_outbreaks(
#'     start_date = "2025-03-01",
#'     end_date = "2025-03-26",
#'     disease_name = "Foot and mouth disease virus (Inf. with) "
#'   )
#'   if (!is.null(fmd_data)) print(utils::head(fmd_data))
#'
#'   # Get all outbreaks in the first week of March 2025
#'   all_data <- get_woah_outbreaks(
#'     start_date = "2025-03-01",
#'     end_date = "2025-03-07",
#'     disease_name = NULL # Or omit the parameter
#'   )
#'   if (!is.null(all_data)) print(utils::head(all_data))
#' }
#' @export
get_woah_outbreaks <- function(start_date,
                               end_date,
                               disease_name = NULL,
                               language = "en",
                               page_size = 100,
                               max_pages = 50,
                               verbose = FALSE) {

  # --- Input Validation ---
  tryCatch({
    start_date_fmt <- format(as.Date(start_date), "%Y-%m-%d")
    end_date_fmt <- format(as.Date(end_date), "%Y-%m-%d")
    if (start_date_fmt > end_date_fmt) {
      stop("start_date cannot be after end_date")
    }
  }, error = function(e){
    stop("Invalid date format or range. Please use YYYY-MM-DD. Error: ", e$message)
  })

  # --- Get Disease ID (using internal helper) ---
  disease_ids <- list() # Use an empty list for "all diseases"
  if (!is.null(disease_name) && nzchar(disease_name)) {
    if (verbose) message("Looking up ID for disease: ", disease_name)
    # Call the internal helper function directly
    disease_ids_result <- get_woah_disease_id(disease_name, language)
    if (is.null(disease_ids_result)) {
      warning("Could not find ID for disease: ", disease_name, ". Fetching for all diseases instead.")
      # Keep disease_ids as an empty list
    } else {
      disease_ids <- disease_ids_result # Assign the list returned by get_woah_disease_id
      if (verbose) message("Found ID(s): ", paste(disease_ids, collapse=", "))
    }
  } else {
    if (verbose) message("Fetching data for all diseases.")
  }


  # --- API Request Setup ---
  api_url <- modify_url("https://wahis.woah.org/api/v1/pi/event/filtered-list",
                        query = list(language = language))

  all_results <- list()
  current_page <- 0
  total_records <- NA # Use NA to indicate we haven't made the first request yet

  if (verbose) message("Starting data fetch...")

  repeat {
    # --- Construct Request Body ---
    request_body_list <- list(
      eventIds = list(),
      reportIds = list(),
      countries = list(),
      firstDiseases = if(length(disease_ids) > 0) I(disease_ids) else list(),
      secondDiseases = list(),
      typeStatuses = list(),
      reasons = list(),
      eventStatuses = list(),
      reportTypes = list(),
      reportStatuses = list(),
      eventStartDate = list(from = start_date_fmt, to = end_date_fmt),
      submissionDate = NULL,
      animalTypes = list(),
      sortColumn = "submissionDate",
      sortOrder = "desc",
      pageSize = page_size,
      pageNumber = current_page
    )

    # --- Explicitly Encode to JSON ---
    json_payload_string <- toJSON(request_body_list, auto_unbox = TRUE, force = TRUE, null = "null")

    if (verbose) {
      message(sprintf("---- Sending JSON Payload (Page %d) ----", current_page))
      # Removed message(prettify(json_payload_string))
      message("-----------------------------")
    }

    # --- Make API Call ---
    page_fetch_success <- FALSE # Flag to track success within tryCatch
    tryCatch({
      response <- POST(
        url = api_url,
        body = json_payload_string,
        add_headers(
          "Content-Type" = "application/json",
          "Accept" = "application/json"
        ),
        timeout(60)
      )

      stop_for_status(response, task = sprintf("fetch page %d", current_page))

      content_raw <- content(response, as = "text", encoding = "UTF-8")

      if (nchar(content_raw) == 0 || !validate(content_raw)) {
        warning(sprintf("Received empty or invalid JSON on page %d. Stopping.", current_page))
        break # Exit repeat loop
      }

      page_data <- fromJSON(content_raw, flatten = TRUE)
      page_fetch_success <- TRUE # Mark as successful

      # Store results
      if (!is.null(page_data$list) && is.data.frame(page_data$list) && nrow(page_data$list) > 0) {
          all_results[[length(all_results) + 1]] <- page_data$list
      } else if (!is.null(page_data$list) && !is.data.frame(page_data$list)) {
          warning(sprintf("API response 'list' on page %d was not a data frame. Skipping.", current_page))
      } else {
        if (verbose) message("No more results found on page ", current_page)
        break # Exit loop if list is empty, null, or not a dataframe with rows
      }

      # Update total records (only needed once) and check exit conditions
      if (is.na(total_records)) {
        total_records <- page_data$totalSize %||% 0 # Use purrr::%||%
        if (verbose) message(sprintf("Total records reported by API: %d", total_records))
      }

      # Check if we've retrieved all expected records or enough pages
      total_fetched <- sum(map_int(all_results, nrow)) # Use purrr::map_int

      if (verbose) message(sprintf("Page %d fetched. Total records so far: %d.", current_page, total_fetched))

      # Exit if we got fewer rows than page size (last page) or fetched all records
      if (nrow(page_data$list) < page_size || total_fetched >= total_records) {
        if (verbose) message("Reached end of data.")
        break # Exit loop
      }

      # --- Prepare for Next Page ---
      current_page <- current_page + 1

      if (current_page >= max_pages) {
        warning("Reached maximum page limit (", max_pages, "). Stopping data fetch. Results may be incomplete.")
        break # Exit loop
      }

      # Optional: Add a small delay to be polite to the server
      # Sys.sleep(0.5)

    }, error = function(e) {
      message(sprintf("\nError during API call for page %d.", current_page))
      # Try to get status code even if response object exists but caused error later
      status_code_val <- NA
      response_content_on_error <- ""
      if (exists("response", inherits = FALSE)) {
         status_code_val <- tryCatch(status_code(response), error = function(e2) NA)
         response_content_on_error <- tryCatch(content(response, as="text", encoding="UTF-8"), error = function(e2) "")
      }
      message("Status Code: ", status_code_val %||% "N/A")
      if (nzchar(response_content_on_error)) {
        message("Response Content on Error:\n", response_content_on_error)
      }
      message("Original R Error: ", e$message)
      # Do not break here, let the flag handle it outside tryCatch
    }) # End tryCatch for API call

    # If the fetch for the current page failed, stop the whole process
    if (!page_fetch_success) {
        message("Aborting pagination due to error on page ", current_page)
        # Decide whether to return partial results or NULL
        # Returning NULL might be safer to indicate failure
        all_results <- list() # Clear any partial results
        break # Exit repeat loop
    }

  } # End repeat loop

  # --- Combine Results ---
  if (length(all_results) == 0) {
    if (verbose) message("No outbreak data successfully fetched or found matching the criteria.")
    # Return empty data frame for type consistency if no error, NULL if error occurred
     return(if(page_fetch_success) data.frame() else NULL)
  }

  if (verbose) message("Combining results...")

  # Use dplyr::bind_rows for robust combination
  combined_data <- tryCatch({
    bind_rows(all_results)
  }, error = function(e){
    message("Error combining results pages: ", e$message)
    message("Returning NULL. Check the structure of individual pages if possible.")
    return(NULL) # Return NULL on combination error
  })

  # --- Data Type Conversion (Optional but Recommended) ---
  if (!is.null(combined_data) && nrow(combined_data) > 0) {
    # Example: Convert date-time columns
    datetime_cols <- c("eventStartDate", "submissionDate") # Add others if needed
    for (col in datetime_cols) {
      if (col %in% names(combined_data)) {
        # Attempt to parse ISO 8601 format
        combined_data[[col]] <- suppressWarnings(
          as.POSIXct(combined_data[[col]], format = "%Y-%m-%dT%H:%M:%S", tz = "UTC")
        )
        # Optional: Convert to Date if time is not needed
        # combined_data[[col]] <- as.Date(combined_data[[col]])
      }
    }
    # Example: Convert potential numeric columns
    numeric_cols <- c("totalReports", "totalOutbreaks") # Add others
     for (col in numeric_cols) {
       if (col %in% names(combined_data)) {
         combined_data[[col]] <- suppressWarnings(as.numeric(combined_data[[col]]))
       }
     }
  }

  if (verbose) message(sprintf("Finished. Returning data frame with %d rows.", nrow(combined_data %||% data.frame())))

  return(combined_data)
}


# --- Helper Function: safe_extract_to_tibble (Internal) ---
# Safely extracts a named element from the main list, converts to tibble, adds IDs.
safe_extract_to_tibble <- function(data_list, element_name, outbreak_id, report_id) {
  element <- purrr::pluck(data_list, element_name)
  id_cols <- list(outbreakId = outbreak_id, reportId = report_id) # Store IDs

  # Special handling for outbreak element
  if (element_name == "outbreak") {
    # Always return at least outbreakId and reportId columns
    if (is.null(element) || !is.list(element) || length(element) == 0) {
      return(tibble::tibble(outbreakId = outbreak_id, reportId = report_id))
    }
    
    # Handle nested outbreak data structure
    if (is.list(element) && !is.data.frame(element)) {
      # Check for common outbreak fields
      outbreak_fields <- c("id", "startDate", "endDate", "description", "epiUnitType")
      result <- tibble::tibble(outbreakId = outbreak_id, reportId = report_id)
      
      # Add available fields
      for (field in outbreak_fields) {
        if (field %in% names(element)) {
          result[[field]] <- element[[field]]
        }
      }
      return(result)
    }
  } else {
    # Handle NULL or non-list elements gracefully for other elements
    if (is.null(element) || !is.list(element)) {
      return(tibble::tibble(outbreakId = integer(0), reportId = integer(0)))
    }
    # Handle empty lists
    if (length(element) == 0) {
      return(tibble::tibble(outbreakId = integer(0), reportId = integer(0)))
    }
  }

  # Attempt conversion based on structure
  df <- tryCatch({
    # Special handling for outbreak element to ensure IDs are included
    if (element_name == "outbreak") {
      result <- if (is.data.frame(element)) {
        tibble::as_tibble(element)
      } else {
        tibble::as_tibble(element)
      }
      # Ensure outbreakId and reportId are set
      if (!"outbreakId" %in% names(result)) {
        result$outbreakId <- outbreak_id
      }
      if (!"reportId" %in% names(result)) {
        result$reportId <- report_id
      }
      return(result)
    }
    
    # Standard handling for other elements
    is_list_of_lists <- all(sapply(element, is.list)) && !is.data.frame(element)
    if (is_list_of_lists) {
      purrr::map_dfr(element, ~ tibble::as_tibble(.x), .id = NULL)
    } else if (!is.data.frame(element)) {
      tibble::as_tibble(element)
    } else {
      tibble::as_tibble(element)
    }
  }, error = function(e) {
    warning(sprintf("Could not convert element '%s' to tibble for outbreak %d, report %d. Error: %s",
                    element_name, outbreak_id, report_id, e$message))
    # Return empty tibble with IDs on conversion error
    return(tibble::tibble(outbreakId = integer(0), reportId = integer(0)))
  })

  # Add IDs if conversion was successful and resulted in rows
  if (nrow(df) > 0) {
    df <- dplyr::bind_cols(tibble::tibble(!!!id_cols, .rows = nrow(df)), df)
    # Optional: Relocate IDs to front if desired, but bind_cols puts them there anyway
    # df <- dplyr::relocate(df, outbreakId, reportId)
  } else {
     # If conversion resulted in 0 rows (e.g. from empty list), ensure ID columns exist
     df <- tibble::tibble(outbreakId = integer(0), reportId = integer(0))
  }

  return(df)
}


# --- Function: get_woah_outbreak_details (Internal Helper) ---

#' Fetch Detailed Information for a Specific Outbreak
#'
#' Retrieves comprehensive details for a specific outbreak from the WOAH API,
#' including outbreak metadata, administrative divisions, species quantities,
#' control measures, and diagnostic methods.
#'
#' @importFrom httr GET content stop_for_status modify_url add_headers timeout status_code
#' @importFrom jsonlite fromJSON validate
#' @importFrom purrr %||%
#' @importFrom tibble tibble as_tibble
#' @importFrom dplyr bind_rows relocate
#' 
#' @param report_id Numeric ID of the report containing the outbreak
#' @param outbreak_id Numeric ID of the specific outbreak
#' @param language Language code (default: "en")
#' @param verbose Logical: Print progress messages? (Default: FALSE)
#'
#' @return A named list of tibbles containing:
#' \itemize{
#'   \item outbreak: Basic outbreak information
#'   \item adminDivisions: Administrative divisions affected
#'   \item speciesQuantities: Species and quantities affected
#'   \item controlMeasures: Control measures implemented
#'   \item diagnosticMethods: Diagnostic methods used
#'   \item additionalMeasures: Additional measures taken
#'   \item measuresNotImplemented: Measures not implemented
#' }
#' @examples
#' \dontrun{
#'   # Get details for outbreak with report ID 123 and outbreak ID 456
#'   details <- get_woah_outbreak_details(123, 456)
#'   print(details$outbreak)
#'   print(details$speciesQuantities)
#' }
#' @export
get_woah_outbreak_details <- function(report_id, outbreak_id, language = "en", verbose = FALSE) {

  # Input validation (keep existing)
  if (!is.numeric(report_id) || length(report_id) != 1 || report_id <= 0) {
    stop("report_id must be a single positive number.")
  }
  if (!is.numeric(outbreak_id) || length(outbreak_id) != 1 || outbreak_id <= 0) {
    stop("outbreak_id must be a single positive number.")
  }

  # Ensure report_id and outbreak_id are integers for consistency
  report_id_int <- as.integer(report_id)
  outbreak_id_int <- as.integer(outbreak_id)

  api_base_url <- "https://wahis.woah.org/api/v1/pi/review/report"
  api_path <- file.path(report_id_int, "outbreak", outbreak_id_int, "all-information")
  api_url <- modify_url(url = file.path(api_base_url, api_path), query = list(language = language))

  if (verbose) message(sprintf("Fetching details for Report ID: %d, Outbreak ID: %d", report_id_int, outbreak_id_int))

  # --- API Call and Initial Parsing ---
  parsed_data <- tryCatch({
      response <- GET(
          url = api_url,
          add_headers("Accept" = "application/json"),
          timeout(60)
      )

      content_raw <- content(response, as = "text", encoding = "UTF-8") # Get raw content first

      if (status_code(response) == 404) {
          if (verbose) message(sprintf("Outbreak details not found (404) for Report ID: %d, Outbreak ID: %d", report_id_int, outbreak_id_int))
          return(NULL) # Return NULL specifically for 404
      }

      # Check for other errors after checking 404
      stop_for_status(response, task = sprintf("fetch outbreak details (Report: %d, Outbreak: %d)", report_id_int, outbreak_id_int))

      if (verbose && nzchar(content_raw)) {
          message(sprintf("--- Raw JSON Response (Report: %d, Outbreak: %d) ---", report_id_int, outbreak_id_int))
          message(substr(content_raw, 1, 1000), if(nchar(content_raw)>1000) "..." else "") # Print truncated raw JSON
          message("-----------------------------------------------------")
      }

      if (nchar(content_raw) == 0 || !validate(content_raw)) {
          warning(sprintf("Received empty or invalid JSON response for outbreak details (Report: %d, Outbreak: %d).", report_id_int, outbreak_id_int))
          # Instead of returning NULL here, let the error handler below catch it
          stop("Empty or invalid JSON content received.")
      }

      # Parse JSON *after* checks
      parsed <- fromJSON(content_raw, flatten = FALSE) # flatten=FALSE initially to better handle nested lists

      if (verbose) message(sprintf("Successfully fetched and parsed details for Report ID: %d, Outbreak ID: %d", report_id_int, outbreak_id_int))
      return(parsed)

  }, error = function(e) {
    # Centralized error handling
    message(sprintf("\nError during API call or parsing for outbreak details (Report: %d, Outbreak: %d).", report_id_int, outbreak_id_int))
    status_code_val <- if (exists("response", inherits = FALSE)) tryCatch(status_code(response), error = function(e2) NA) else NA
    response_content_on_error <- if (exists("content_raw", inherits = FALSE)) content_raw else ""

    message("Status Code: ", status_code_val %||% "N/A")
    if (nzchar(response_content_on_error)) {
        message("--- Raw Content on Error ---")
        message(substr(response_content_on_error, 1, 500), if(nchar(response_content_on_error)>500) "..." else "")
        message("--------------------------")
    }
    message("Original R Error: ", e$message)
    return(NULL) # Return NULL on any error during fetch/parse
  })

  # If API call or parsing failed, return NULL
  if (is.null(parsed_data)) {
    return(NULL)
  }

  # --- Extract Components into Tibbles using the helper ---
  if (verbose) message("Extracting components into tibbles...")

  # Define the elements to extract
  elements_to_extract <- c(
    "outbreak", "adminDivisions", "quantityUnit", "speciesQuantities",
    "controlMeasures", "diagnosticMethods", "additionalMeasures",
    "measuresNotImplemented"
  )

  # Use purrr::map to apply the helper function
  details_list <- purrr::map(elements_to_extract, ~ safe_extract_to_tibble(
                                                        data_list = parsed_data,
                                                        element_name = .x,
                                                        outbreak_id = outbreak_id_int,
                                                        report_id = report_id_int
                                                      )) %>%
                  purrr::set_names(elements_to_extract) # Name the list elements

  # --- Specific Post-processing for nested structures ---

  # Post-process speciesQuantities to unnest further if it exists and has rows
  if ("speciesQuantities" %in% names(details_list) && nrow(details_list$speciesQuantities) > 0) {
      if(verbose) message("Post-processing speciesQuantities...")
      sq_raw <- details_list$speciesQuantities # Keep original IDs

      # Define a function to safely unnest and select, handling NULLs
      safe_unnest <- function(df, col_to_unnest) {
          if (!col_to_unnest %in% names(df)) return(tibble::tibble()) # Return empty if col missing
          # Ensure the column is list-like before unnesting
          if (!is.list(df[[col_to_unnest]])) return(tibble::tibble())

          # Use tryCatch around unnest_wider
          tryCatch({
              tidyr::unnest_wider(df, col = {{col_to_unnest}}, names_sep = "_")
          }, error = function(e) {
              warning(sprintf("Failed to unnest column '%s': %s", col_to_unnest, e$message))
              return(tibble::tibble()) # Return empty tibble on error
          })
      }

      # Unnest step-by-step, preserving IDs
      sq_total <- safe_unnest(sq_raw, "totalQuantities")
      sq_new <- safe_unnest(sq_raw, "newQuantities")
      sq_type <- safe_unnest(sq_raw, "speciesType")

      # Combine the unnested parts with the original IDs
      # Start with IDs
      sq_processed <- sq_raw %>% select(outbreakId, reportId)

      # Add columns from unnested parts if they exist and have rows
      if (nrow(sq_total) > 0) sq_processed <- dplyr::bind_cols(sq_processed, sq_total %>% select(-any_of(c("outbreakId", "reportId"))))
      if (nrow(sq_new) > 0) sq_processed <- dplyr::bind_cols(sq_processed, sq_new %>% select(-any_of(c("outbreakId", "reportId"))))
      if (nrow(sq_type) > 0) sq_processed <- dplyr::bind_cols(sq_processed, sq_type %>% select(-any_of(c("outbreakId", "reportId"))))

      # Assign back to the list
      details_list$speciesQuantities <- sq_processed
  } else {
      # Ensure speciesQuantities exists even if empty, with ID columns
      details_list$speciesQuantities <- tibble::tibble(outbreakId = integer(0), reportId = integer(0))
  }

  # Similar post-processing for 'outbreak' if needed (e.g., unnest epiUnitType, description)
  if ("outbreak" %in% names(details_list) && nrow(details_list$outbreak) > 0) {
      if(verbose) message("Post-processing outbreak...")
      details_list$outbreak <- details_list$outbreak %>%
          tidyr::unnest_wider(any_of(c("epiUnitType", "description")), names_sep = "_", names_repair = "unique") %>%
          # Rename potentially duplicated columns after unnesting
          dplyr::rename_with(~gsub("\\.\\.\\.[0-9]+$", "", .), dplyr::matches("\\.\\.\\.[0-9]+$"))
  } else {
       details_list$outbreak <- tibble::tibble(outbreakId = integer(0), reportId = integer(0))
  }

   # Post-process diagnosticMethods if needed
  if ("diagnosticMethods" %in% names(details_list) && nrow(details_list$diagnosticMethods) > 0) {
      if(verbose) message("Post-processing diagnosticMethods...")
      details_list$diagnosticMethods <- details_list$diagnosticMethods %>%
          tidyr::unnest_wider(any_of("nature"), names_sep = "_", names_repair = "unique") %>%
          dplyr::rename_with(~gsub("\\.\\.\\.[0-9]+$", "", .), dplyr::matches("\\.\\.\\.[0-9]+$"))
  } else {
       details_list$diagnosticMethods <- tibble::tibble(outbreakId = integer(0), reportId = integer(0))
  }

  # Ensure all expected tables exist in the final list, even if empty
  expected_tables <- c("outbreak", "adminDivisions", "quantityUnit", "speciesQuantities",
                       "controlMeasures", "diagnosticMethods", "additionalMeasures",
                       "measuresNotImplemented")
  for (tbl_name in expected_tables) {
      if (!tbl_name %in% names(details_list)) {
          details_list[[tbl_name]] <- tibble::tibble(outbreakId = integer(0), reportId = integer(0))
          if(verbose) message(sprintf("Added empty tibble for missing element: %s", tbl_name))
      }
  }


  if (verbose) message("Finished extracting components.")
  return(details_list)
}


# --- Function: get_woah_outbreaks_full_info ---

#' Fetch Full Outbreak Information (Events, Locations, Details) as Separate Tables
#'
#' Retrieves comprehensive outbreak information by fetching event and location data,
#' linking them, querying detailed information for each specific outbreak using
#' `get_woah_outbreak_details`, and finally combining the results into a named list
#' of tibbles, where each tibble corresponds to a specific part of the outbreak data
#' (e.g., 'outbreak', 'adminDivisions', 'speciesQuantities').
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
#' @return A named list of tibbles. Each element in the list corresponds to a
#'   component of the outbreak details (e.g., `outbreak`, `adminDivisions`,
#'   `speciesQuantities`, `diagnosticMethods`, etc.), containing the combined data
#'   from all successfully fetched outbreaks. Returns an empty named list if no
#'   outbreaks match or if errors occur during fetching. Returns NULL if the
#'   initial event or location fetch fails critically.
#' @examples
#' \dontrun{
#'   # Get full details for HPAI outbreaks in March 2025
#'   hpai_full_details_list <- get_woah_outbreaks_full_info(
#'     start_date = "2025-03-01",
#'     end_date = "2025-03-26",
#'     disease_name = "Influenza A virus (Inf. with high pathogenicity) (OIE-listed)"
#'   )
#'   if (length(hpai_full_details_list) > 0 &&
#'       !is.null(hpai_full_details_list$outbreak) &&
#'       nrow(hpai_full_details_list$outbreak) > 0) {
#'     print(paste("Fetched details for", nrow(hpai_full_details_list$outbreak), "outbreaks."))
#'     print("Available tables:")
#'     print(names(hpai_full_details_list))
#'     print("Structure of outbreak table:")
#'     dplyr::glimpse(hpai_full_details_list$outbreak)
#'     print("Structure of speciesQuantities table:")
#'     dplyr::glimpse(hpai_full_details_list$speciesQuantities)
#'   } else {
#'     print("No outbreak details found or error occurred.")
#'   }
#' }
#' @export
get_woah_outbreaks_full_info <- function(start_date,
                                         end_date,
                                         disease_name = NULL,
                                         language = "en",
                                         verbose = FALSE) {

  # Define expected table names based on get_woah_outbreak_details output
  expected_tables <- c(
    "outbreak", "adminDivisions", "quantityUnit", "speciesQuantities",
    "controlMeasures", "diagnosticMethods", "additionalMeasures",
    "measuresNotImplemented"
  )
  # Initialize result list with empty tibbles for expected structure
  empty_result_list <- purrr::map(expected_tables, ~ tibble::tibble()) %>%
                       purrr::set_names(expected_tables)

  if (verbose) message("--- Starting Full Outbreak Information Fetch (Multi-Table Output) ---")

  # --- 1. Fetch Outbreak Events ---
  if (verbose) message("Step 1: Fetching outbreak events...")
  outbreak_events <- get_woah_outbreaks(start_date = start_date, end_date = end_date, disease_name = disease_name, language = language, verbose = verbose)
  if (is.null(outbreak_events)) {
    message("Error: Failed to fetch initial outbreak event data. Aborting.")
    return(NULL) # Critical failure
  }
  if (nrow(outbreak_events) == 0) {
    if (verbose) message("No outbreak events found matching criteria.")
    return(empty_result_list) # Return empty list structure
  }
  if (verbose) message(sprintf("Found %d outbreak events.", nrow(outbreak_events)))

  # --- 2. Fetch Outbreak Locations ---
  if (verbose) message("Step 2: Fetching outbreak locations...")
  outbreak_locations <- get_woah_outbreak_locations(start_date = start_date, end_date = end_date, disease_name = disease_name, language = language, verbose = verbose)
  if (is.null(outbreak_locations)) {
    # Non-critical? Could proceed without locations, but likely indicates broader issues.
    # Let's treat it as critical for now.
    message("Error: Failed to fetch outbreak location data. Aborting.")
    return(NULL) # Critical failure
  }
  if (nrow(outbreak_locations) == 0) {
    if (verbose) message("No outbreak locations found matching criteria.")
    return(empty_result_list) # Return empty list structure
  }
  if (verbose) message(sprintf("Found %d outbreak location records.", nrow(outbreak_locations)))

  # --- 3. Link Events and Locations to get Report/Outbreak Pairs ---
  if (verbose) message("Step 3: Linking events and locations...")
  # Input validation for required columns
  if (!all(c("eventId", "reportId") %in% names(outbreak_events))) {
    abort("Required columns 'eventId' or 'reportId' missing from events data.")
  }
  if (!all(c("eventId", "outbreakId") %in% names(outbreak_locations))) {
    abort("Required columns 'eventId' or 'outbreakId' missing from locations data.")
  }

  # Select necessary columns and join
  events_subset <- outbreak_events %>% select(eventId, reportId) %>% distinct()
  locations_subset <- outbreak_locations %>% select(eventId, outbreakId) %>% distinct()
  report_outbreak_pairs <- inner_join(events_subset, locations_subset, by = "eventId", relationship = "many-to-many") %>%
                           select(reportId, outbreakId) %>%
                           distinct()

  if (nrow(report_outbreak_pairs) == 0) {
    if (verbose) message("No matching event/location pairs found after join.")
    return(empty_result_list) # Return empty list structure
  }
  n_pairs <- nrow(report_outbreak_pairs)
  if (verbose) message(sprintf("Found %d unique report/outbreak pairs to fetch details for.", n_pairs))

  # --- 4. Fetch Details for Each Pair using the updated get_woah_outbreak_details ---
  if (verbose) message("Step 4: Fetching full details (as list of tables) for each outbreak...")
  # Use the updated internal function which returns a list of tibbles or NULL
  possibly_get_details_list <- possibly(get_woah_outbreak_details, otherwise = NULL, quiet = !verbose)

  # This will be a list, where each element is *either* NULL *or* a named list of tibbles
  all_details_results <- map2(
    report_outbreak_pairs$reportId,
    report_outbreak_pairs$outbreakId,
    ~ possibly_get_details_list(
        report_id = .x,
        outbreak_id = .y,
        language = language,
        verbose = FALSE # Keep inner call quiet unless debugging needed
      ),
    .progress = verbose
  )

  # Filter out the NULL results (where fetching failed for an outbreak)
  successful_details_lists <- discard(all_details_results, is.null)
  n_successful <- length(successful_details_lists)
  n_failed <- n_pairs - n_successful

  if (verbose) message(sprintf("Successfully fetched detail lists for %d outbreaks.", n_successful))
  if (n_failed > 0 && verbose) message(sprintf("Failed to fetch details for %d outbreaks.", n_failed))

  if (n_successful == 0) {
    if (verbose) message("No details fetched successfully for any outbreak.")
    return(empty_result_list) # Return empty list structure
  }

  # --- 5. Combine Corresponding Tibbles Across Outbreaks ---
  if (verbose) message("Step 5: Combining corresponding tables across all fetched outbreaks...")

  # Get the names of the tables from the first successful result (should be consistent)
  # Use expected_tables defined earlier for robustness
  table_names <- expected_tables

  # Use map to iterate through table names, pluck the corresponding tibble from each list, and bind_rows
  combined_tables_list <- map(table_names, function(tbl_name) {
    if (verbose) message(sprintf("  Combining table: '%s'", tbl_name))
    # Pluck the tibble with name `tbl_name` from each element in `successful_details_lists`
    list_of_tibbles_for_name <- map(successful_details_lists, ~ purrr::pluck(.x, tbl_name))

    # Filter out any NULLs or non-dataframes that might have slipped through (shouldn't happen with safe_extract_to_tibble)
    valid_tibbles <- keep(list_of_tibbles_for_name, ~ is.data.frame(.x) && nrow(.x) > 0)

    if (length(valid_tibbles) == 0) {
        if (verbose) message(sprintf("    No data found for table '%s' in any outbreak.", tbl_name))
        # Return an empty tibble with outbreakId and reportId columns
        return(tibble::tibble(outbreakId = integer(0), reportId = integer(0)))
    }

    # Combine the valid tibbles using bind_rows (handles differing columns by filling with NA)
    combined_tbl <- tryCatch({
        result <- dplyr::bind_rows(valid_tibbles)
        # Ensure outbreakId and reportId columns exist
        if (!all(c("outbreakId", "reportId") %in% names(result))) {
          if ("outbreakId" %in% names(result)) {
            result <- dplyr::mutate(result, reportId = NA_integer_)
          } else if ("reportId" %in% names(result)) {
            result <- dplyr::mutate(result, outbreakId = NA_integer_)
          } else {
            result <- dplyr::mutate(result, 
                                   outbreakId = NA_integer_,
                                   reportId = NA_integer_)
          }
        }
        # Move ID columns to front
        dplyr::relocate(result, outbreakId, reportId)
    }, error = function(e) {
        warning(sprintf("Error combining tibbles for '%s': %s. Returning empty tibble.", tbl_name, e$message))
        # Return empty tibble with IDs on error
        return(tibble::tibble(outbreakId = integer(0), reportId = integer(0)))
    })

    if (verbose) message(sprintf("    Combined '%s' table has %d rows.", tbl_name, nrow(combined_tbl)))
    return(combined_tbl)

  }) %>% set_names(table_names) # Set the names of the final list


  # --- 6. Final Result ---
  # The result is the named list of combined tibbles
  if (verbose) message("Finished processing. Returning list of combined tables.")
  if (verbose) message("--- Full Outbreak Information Fetch Complete (Multi-Table Output) ---")

  # Ensure all tables have outbreakId and reportId columns
  final_result <- map(combined_tables_list, function(tbl) {
    if (!all(c("outbreakId", "reportId") %in% names(tbl))) {
      if (nrow(tbl) > 0) {
        tbl <- dplyr::mutate(tbl, 
                            outbreakId = if ("outbreakId" %in% names(tbl)) outbreakId else NA_integer_,
                            reportId = if ("reportId" %in% names(tbl)) reportId else NA_integer_)
      }
      dplyr::relocate(tbl, outbreakId, reportId)
    } else {
      tbl
    }
  }) %>% set_names(table_names)

  return(final_result)
}
