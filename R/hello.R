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


# --- Function: get_woah_outbreak_details (Internal Helper) ---

#' Fetch Full Details for a Single Outbreak
#'
#' Retrieves the complete information set for a specific outbreak using its
#' report ID and outbreak ID from the WOAH API. This is typically used as a
#' helper function.
#'
#' @importFrom httr GET modify_url add_headers stop_for_status content timeout status_code
#' @importFrom jsonlite fromJSON validate
#' @importFrom purrr %||%
#'
#' @param report_id The numeric ID of the report containing the outbreak.
#' @param outbreak_id The numeric ID of the specific outbreak.
#' @param language Language code (default: "en").
#' @param verbose Logical: Print progress messages? (Default: FALSE)
#'
#' @return A list containing the detailed outbreak information, or NULL if an error occurs
#'   or the outbreak is not found.
#' @noRd
get_woah_outbreak_details <- function(report_id, outbreak_id, language = "en", verbose = FALSE) {

  # Input validation
  if (!is.numeric(report_id) || length(report_id) != 1 || report_id <= 0) {
    stop("report_id must be a single positive number.")
  }
  if (!is.numeric(outbreak_id) || length(outbreak_id) != 1 || outbreak_id <= 0) {
    stop("outbreak_id must be a single positive number.")
  }

  api_base_url <- "https://wahis.woah.org/api/v1/pi/review/report"
  api_path <- file.path(report_id, "outbreak", outbreak_id, "all-information")
  api_url <- modify_url(url = file.path(api_base_url, api_path), query = list(language = language))

  if (verbose) message(sprintf("Fetching details for Report ID: %d, Outbreak ID: %d", report_id, outbreak_id))

  details_data <- tryCatch({
    response <- GET(
      url = api_url,
      add_headers("Accept" = "application/json"),
      timeout(60) # Increased timeout for potentially larger response
    )

    # Check specifically for 404 Not Found
    if (status_code(response) == 404) {
       if (verbose) message(sprintf("Outbreak details not found (404) for Report ID: %d, Outbreak ID: %d", report_id, outbreak_id))
       return(NULL) # Return NULL specifically for 404
    }

    # Check for other HTTP errors
    stop_for_status(response, task = sprintf("fetch outbreak details (Report: %d, Outbreak: %d)", report_id, outbreak_id))

    content_raw <- content(response, as = "text", encoding = "UTF-8")

    if (nchar(content_raw) == 0 || !validate(content_raw)) {
      warning(sprintf("Received empty or invalid JSON response for outbreak details (Report: %d, Outbreak: %d).", report_id, outbreak_id))
      return(NULL)
    }

    parsed_data <- fromJSON(content_raw, flatten = TRUE) # Flatten might be useful here too

    if (verbose) message(sprintf("Successfully fetched details for Report ID: %d, Outbreak ID: %d", report_id, outbreak_id))

    return(parsed_data)

  }, error = function(e) {
    message(sprintf("\nError during API call for outbreak details (Report: %d, Outbreak: %d).", report_id, outbreak_id))
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
    return(NULL) # Return NULL on error
  })

  return(details_data)
}


# --- Function: get_woah_outbreaks_full_info ---

#' Fetch Full Outbreak Information (Events, Locations, Details)
#'
#' Retrieves comprehensive outbreak information by first fetching event and location data,
#' linking them, and then querying detailed information for each specific outbreak.
#'
#' @importFrom dplyr inner_join select distinct rename
#' @importFrom purrr map2 possibly list_rbind
#' @importFrom tidyr unnest_wider unnest_longer
#' @importFrom tibble as_tibble
#'
#' @param start_date Character string or Date object for the start of the event date range (YYYY-MM-DD).
#' @param end_date Character string or Date object for the end of the event date range (YYYY-MM-DD).
#' @param disease_name Optional: Character string specifying the exact disease name.
#'   If NULL or empty, fetches data for all diseases.
#' @param language Language code (default: "en").
#' @param verbose Logical: Print progress messages? (Default: FALSE)
#'
#' @return A list of lists, where each inner list contains the full details for one outbreak.
#'   Returns an empty list if no outbreaks match the criteria or if errors occur during fetching.
#'   Returns NULL if the initial event or location fetch fails critically.
#' @examples
#' \dontrun{
#'   # Get full details for Foot and Mouth disease outbreaks in March 2025
#'   fmd_full_details <- get_woah_outbreaks_full_info(
#'     start_date = "2025-03-01",
#'     end_date = "2025-03-26",
#'     disease_name = "Foot and mouth disease virus (Inf. with) "
#'   )
#'   if (length(fmd_full_details) > 0) {
#'     print(paste("Fetched details for", length(fmd_full_details), "outbreaks."))
#'     # Inspect the first outbreak's details
#'     # str(fmd_full_details[[1]], max.level = 2)
#'   }
#'
#'   # Get full details for all outbreaks in the first week of March 2025
#'   all_full_details <- get_woah_outbreaks_full_info(
#'     start_date = "2025-03-01",
#'     end_date = "2025-03-07",
#'     disease_name = NULL
#'   )
#'    if (length(all_full_details) > 0) {
#'     print(paste("Fetched details for", length(all_full_details), "outbreaks."))
#'   }
#' }
#' @export
get_woah_outbreaks_full_info <- function(start_date,
                                         end_date,
                                         disease_name = NULL,
                                         language = "en",
                                         verbose = FALSE) {

  if (verbose) message("--- Starting Full Outbreak Information Fetch ---")

  # --- 1. Fetch Outbreak Events ---
  if (verbose) message("Step 1: Fetching outbreak events...")
  outbreak_events <- get_woah_outbreaks(
    start_date = start_date,
    end_date = end_date,
    disease_name = disease_name,
    language = language,
    verbose = verbose # Pass verbosity down
  )

  if (is.null(outbreak_events)) {
    message("Error: Failed to fetch initial outbreak event data. Aborting.")
    return(NULL)
  }
  if (nrow(outbreak_events) == 0) {
    if (verbose) message("No outbreak events found for the specified criteria.")
    return(list()) # Return empty list if no events
  }
  if (verbose) message(sprintf("Found %d outbreak events.", nrow(outbreak_events)))

  # --- 2. Fetch Outbreak Locations ---
  if (verbose) message("Step 2: Fetching outbreak locations...")
  outbreak_locations <- get_woah_outbreak_locations(
    start_date = start_date,
    end_date = end_date,
    disease_name = disease_name,
    language = language,
    verbose = verbose # Pass verbosity down
  )

  if (is.null(outbreak_locations)) {
    message("Error: Failed to fetch outbreak location data. Aborting.")
    # Consider if partial results from events should be returned, but safer to abort.
    return(NULL)
  }
   if (nrow(outbreak_locations) == 0) {
    if (verbose) message("No outbreak locations found for the specified criteria (although events were found).")
    return(list()) # Return empty list if no locations
  }
  if (verbose) message(sprintf("Found %d outbreak location records.", nrow(outbreak_locations)))

  # --- 3. Link Events and Locations ---
  if (verbose) message("Step 3: Linking events and locations...")

  # Ensure necessary columns exist
  if (!"eventId" %in% names(outbreak_events) || !"reportId" %in% names(outbreak_events)) {
     message("Error: 'eventId' or 'reportId' missing from outbreak events data.")
     return(list())
  }
   if (!"eventId" %in% names(outbreak_locations) || !"outbreakId" %in% names(outbreak_locations)) {
     message("Error: 'eventId' or 'outbreakId' missing from outbreak locations data.")
     return(list())
  }

  # Select relevant columns and join
  events_subset <- outbreak_events %>%
    select(eventId, reportId) %>%
    distinct() # Ensure unique event-report pairs if duplicates exist

  locations_subset <- outbreak_locations %>%
    select(eventId, outbreakId) %>%
    distinct() # Ensure unique event-outbreak pairs

  # Join to get reportId and outbreakId pairs
  # Need to handle cases where eventId might be in one but not the other? Inner join is safest.
  report_outbreak_pairs <- inner_join(events_subset, locations_subset, by = "eventId")

  if (nrow(report_outbreak_pairs) == 0) {
    if (verbose) message("No matching event/location pairs found after joining.")
    return(list())
  }

  # Ensure unique reportId-outbreakId pairs before fetching details
  report_outbreak_pairs <- report_outbreak_pairs %>%
    select(reportId, outbreakId) %>%
    distinct()

  n_pairs <- nrow(report_outbreak_pairs)
  if (verbose) message(sprintf("Found %d unique report/outbreak pairs to fetch details for.", n_pairs))

  # --- 4. Fetch Details for Each Pair ---
  if (verbose) message("Step 4: Fetching full details for each outbreak...")

  # Create a 'possibly' version of the details function to handle errors gracefully
  possibly_get_details <- possibly(get_woah_outbreak_details, otherwise = NULL, quiet = !verbose)

  # Use map2 to iterate over reportId and outbreakId simultaneously
  all_details_list <- map2(
    report_outbreak_pairs$reportId,
    report_outbreak_pairs$outbreakId,
    ~ possibly_get_details(
        report_id = .x,
        outbreak_id = .y,
        language = language,
        verbose = verbose
      ),
    .progress = verbose # Show progress bar if verbose
  )

  # Filter out NULL results (errors or 404s)
  successful_details <- Filter(Negate(is.null), all_details_list)
  n_successful <- length(successful_details)
  n_failed <- n_pairs - n_successful

  if (verbose) {
      message(sprintf("Successfully fetched details for %d outbreaks.", n_successful))
      if (n_failed > 0) {
          message(sprintf("Failed to fetch details for %d outbreaks (check logs for errors/404s).", n_failed))
      }
  }

  if (verbose) message("--- Full Outbreak Information Fetch Complete ---")

  return(successful_details)
}
