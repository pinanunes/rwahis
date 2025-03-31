#' Update WOAH Outbreak Data in a PostgreSQL/PostGIS Database (Refactored)
#'
#' Fetches all outbreak data (locations and full details) from the WOAH API for a
#' specified date range first, then connects to the database to upsert the
#' combined data into corresponding tables in a PostgreSQL database with the
#' PostGIS extension. Tables are created if they don't exist.
#'
#' @details
#' This refactored function performs the following steps:
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
#'       \item Combines the data from all batches.
#'       \item Connects to the specified PostgreSQL database.
#'       \item Defines target table names (prefixed with `table_prefix`).
#'       \item Checks if each target table exists. If not, it creates the table.
#'       \item Performs database write operations within a transaction:
#'         \itemize{
#'           \item For each combined data table (locations, outbreak, speciesQuantities, etc.):
#'             \itemize{
#'               \item Identifies new records based on primary keys (existing records are skipped).
#'               \item Appends only the new records to the corresponding database table.
#'               \item Location data is converted to `sf` objects before writing.
#'             }
#'         }
#'       \item Disconnects from the database.
#'     }
#' }
#'
#' **Required Database Setup:** (Same as before)
#' \itemize{
#'   \item PostgreSQL server.
#'   \item PostGIS extension enabled (`CREATE EXTENSION postgis;`).
#'   \item User specified in `db_params` needs connection and table creation/write permissions.
#' }
#'
#' **Table Schema Notes:** (Same as before)
#' \itemize{
#'  \item `locations`: PK: `outbreak_id`.
#'  \item `outbreak`: PK: `outbreak_id`.
#'  \item `quantity_unit`: PK: `outbreak_id`.
#'  \item `admin_divisions`: PK: `outbreak_id`, `area_id`.
#'  \item `species_quantities`: PK: `outbreak_id`, `species_id`.
#'  \item `control_measures`: PK: `outbreak_id`, `measure_id`.
#'  \item `diagnostic_methods`: PK: `outbreak_id`, `nature_id`.
#' }
#'
#' @param start_date Character string or Date object for the start of the overall date range (YYYY-MM-DD).
#' @param end_date Character string or Date object for the end of the overall date range (YYYY-MM-DD).
#' @param disease_name Optional: Character string specifying the exact disease name to filter by.
#'   If NULL (default), fetches data for all diseases.
#' @param db_params A list containing database connection parameters:
#'   \itemize{
#'     \item `dbname`: Database name
#'     \item `host`: Database host
#'     \item `port`: Database port
#'     \item `user`: Database user
#'     \item `password`: Database password
#'   }
#' @param batch_interval Character string specifying the interval for batch processing
#'   (e.g., "1 month", "2 weeks", "10 days"). Default is "1 month". Set to `NULL`
#'   to process the entire range at once (not recommended for large ranges).
#' @param table_prefix Character string prefix for all created/updated database tables. Default is "wahis_".
#' @param language Language code for API requests (default: "en").
#' @param verbose Logical: Print progress messages? (Default: TRUE for this function).
#'
#' @return Invisibly returns TRUE if completed (potentially with warnings), or FALSE if a critical error occurred during fetching or writing.
#'
#' @importFrom DBI dbConnect dbDisconnect dbExistsTable dbExecute dbGetQuery dbWriteTable Id dbWithTransaction
#' @importFrom RPostgres Postgres
#' @importFrom sf st_as_sf st_write st_geometry st_crs<- st_crs
#' @importFrom lubridate ymd interval duration floor_date %m+% months days weeks
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
#' # Database connection parameters (replace with your actual details)
#' db_connection_params <- list(
#'   dbname = "animal_health_db",
#'   host = "localhost",
#'   port = 5432,
#'   user = "db_user",
#'   password = "your_password"
#' )
#'
#' # Update HPAI data for March 2025 in monthly batches
#' update_woah_db(
#'   start_date = "2025-03-01",
#'   end_date = "2025-03-31",
#'   disease_name = "Influenza A virus (Inf. with high pathogenicity) (OIE-listed)",
#'   db_params = db_connection_params,
#'   batch_interval = "1 month"
#' )
#'
#' # Update all FMD data for Q1 2025 in 2-week batches
#' update_woah_db(
#'   start_date = "2025-01-01",
#'   end_date = "2025-03-31",
#'   disease_name = "Foot and mouth disease virus (Inf. with) ",
#'   db_params = db_connection_params,
#'   batch_interval = "2 weeks",
#'   table_prefix = "fmd_"
#' )
#' }
update_woah_db <- function(start_date,
                           end_date,
                           disease_name = NULL,
                           db_params,
                           batch_interval = "1 month",
                           table_prefix = "wahis_",
                           language = "en",
                           verbose = TRUE) {

  # --- Input Validation ---
  if (missing(db_params) || !is.list(db_params) || !all(c("dbname", "host", "port", "user", "password") %in% names(db_params))) {
    stop("`db_params` list must contain dbname, host, port, user, and password.")
  }
  start_date <- tryCatch(lubridate::ymd(start_date), error = function(e) stop("Invalid start_date format. Use YYYY-MM-DD."))
  end_date <- tryCatch(lubridate::ymd(end_date), error = function(e) stop("Invalid end_date format. Use YYYY-MM-DD."))
  if (start_date > end_date) {
    stop("start_date cannot be after end_date.")
  }

  # --- Define Table Names ---
  # Define table names early for potential use in schema definitions if needed outside connection block
   table_names <- list(
    locations = paste0(table_prefix, "locations"),
    outbreak = paste0(table_prefix, "outbreak"),
    quantity_unit = paste0(table_prefix, "quantity_unit"),
    admin_divisions = paste0(table_prefix, "admin_divisions"),
    species_quantities = paste0(table_prefix, "species_quantities"),
    control_measures = paste0(table_prefix, "control_measures"),
    diagnostic_methods = paste0(table_prefix, "diagnostic_methods"),
    additional_measures = paste0(table_prefix, "additional_measures"), # Assuming structure if used
    measures_not_implemented = paste0(table_prefix, "measures_not_implemented") # Assuming structure if used
  )

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
    # Ensure end date is not before start date (can happen with large intervals near end date)
    valid_intervals <- start_dates <= end_dates_calc
    start_dates <- start_dates[valid_intervals]
    end_dates_final <- end_dates_calc[valid_intervals]


    if(length(start_dates) == 0) {
        message("No valid date intervals generated based on start/end dates and batch interval. Nothing to process.")
        return(invisible(TRUE))
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
        safe_get_locations <- purrr::safely(get_woah_outbreak_locations)
        locations_result <- safe_get_locations(
            start_date = int_start,
            end_date = int_end,
            disease_name = disease_name,
            language = language,
            verbose = verbose # Pass through verbose setting
        )

        if (!is.null(locations_result$error)) {
            warning(sprintf("Error fetching locations for interval %s - %s: %s. Skipping batch.", int_start, int_end, locations_result$error$message))
            fetch_error_current_batch <- TRUE
        } else if (is.null(locations_result$result) || nrow(locations_result$result) == 0) {
            if (verbose) message("No locations found for this interval.")
            # Store NULL or empty placeholder? Let's store NULL to signify no data for this batch.
            batch_locations <- NULL
            batch_details <- NULL # No point fetching details if no locations
        } else {
             batch_locations <- locations_result$result
             if (verbose) message(sprintf("Fetched %d location records.", nrow(batch_locations)))

             if (verbose) message("Fetching full outbreak details...")
             safe_get_details <- purrr::safely(get_woah_outbreaks_full_info)
             details_result <- safe_get_details(
                 start_date = int_start,
                 end_date = int_end,
                 disease_name = disease_name,
                 language = language,
                 verbose = verbose # Pass through verbose setting
             )

             if (!is.null(details_result$error)) {
                 warning(sprintf("Error fetching full details for interval %s - %s: %s. Details for this batch might be incomplete.", int_start, int_end, details_result$error$message))
                 # Decide: store NULL details or proceed with just locations? Let's store NULL.
                 batch_details <- NULL
                 fetch_error_current_batch <- TRUE # Mark error even if locations were fetched
             } else if (is.null(details_result$result) || length(details_result$result) == 0) {
                 if (verbose) message("No outbreak details found for this interval (despite locations existing).")
                 batch_details <- NULL # Store NULL
             } else {
                 batch_details <- details_result$result
                 if (verbose) {
                     # Calculate total records safely, handling potential NULL data frames within the list
                     total_records_in_details <- sum(sapply(batch_details, function(x) if(is.data.frame(x)) nrow(x) else 0))
                     message(sprintf("  Retrieved details with %d total records across all tables.", total_records_in_details))
                 }
             }
        }
    }, error = function(e) {
        # Catch unexpected errors during the tryCatch block itself
        warning(sprintf("Unexpected error during data fetching for interval %s - %s: %s. Skipping batch.", int_start, int_end, e$message))
        fetch_error_current_batch <<- TRUE # Assign to outer scope if needed, but should be caught by safely
    })

    # Store results if fetch was successful for the batch (or partially successful)
    if (!fetch_error_current_batch) {
        all_locations_list[[i]] <- batch_locations # Could be NULL or data frame
        all_details_list[[i]] <- batch_details     # Could be NULL or list of data frames
    } else {
        all_locations_list[[i]] <- NULL # Ensure placeholder for failed batch
        all_details_list[[i]] <- NULL
        any_fetch_error <- TRUE # Mark that at least one batch had issues
    }

  } # End loop through date_intervals

  if (verbose) message("\n--- Data Fetching Phase Complete ---")

  # --- Combine Data ---
  # Remove NULL elements resulting from failed batches before binding
  valid_locations <- purrr::compact(all_locations_list)
  valid_details <- purrr::compact(all_details_list)

  if (length(valid_locations) == 0 && length(valid_details) == 0) {
      message("No data successfully fetched from any batch. Nothing to write to the database.")
      # Return TRUE because fetching completed (albeit with no data), but FALSE if fetch errors occurred
      return(invisible(!any_fetch_error))
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
  } else {
      if (verbose) message("No valid location data fetched.")
  }


  # Combine details (more complex list structure)
  combined_details_list <- NULL
  if (length(valid_details) > 0) {
      # Get names from the first valid details list (assuming structure is consistent)
      detail_names <- names(valid_details[[1]])
      combined_details_list <- tryCatch({
          purrr::map(detail_names, function(name) {
              # Extract the data frame for 'name' from each batch's list, compact NULLs, then bind
              batch_dfs <- purrr::map(valid_details, ~ purrr::pluck(.x, name)) %>% purrr::compact()
              if (length(batch_dfs) > 0) {
                  # Check if all elements are data frames before binding
                  if(all(purrr::map_lgl(batch_dfs, is.data.frame))) {
                      dplyr::bind_rows(batch_dfs)
                  } else {
                      warning(sprintf("Non-data frame element found when combining details for '%s'. Skipping combination for this element.", name))
                      NULL # Return NULL if structure is inconsistent
                  }
              } else {
                  NULL # Return NULL if no data for this element across batches
              }
          }) %>% purrr::set_names(detail_names) %>%
          purrr::compact() # Remove elements that ended up NULL
      }, error = function(e) {
          warning("Error combining details lists: ", e$message)
          NULL
      })
       if (!is.null(combined_details_list) && verbose) {
           total_combined_details <- sum(sapply(combined_details_list, function(x) if(is.data.frame(x)) nrow(x) else 0))
           message(sprintf("Combined details lists from %d batches, resulting in %d total records across %d tables.",
                           length(valid_details), total_combined_details, length(combined_details_list)))
       }
  } else {
       if (verbose) message("No valid details data fetched.")
  }


  # Check if we have any combined data to write
  if (is.null(combined_locations_data) && is.null(combined_details_list)) {
      message("No combined data available to write to the database.")
      return(invisible(!any_fetch_error)) # Success if no fetch errors, FALSE otherwise
  }


  # --- Database Operations ---
  if (verbose) message("\n--- Starting Database Writing Phase ---")
  conn <- NULL # Initialize conn
  db_write_success <- FALSE # Flag for overall DB operation success

  # Ensure disconnection even if connection or writing fails
  on.exit({
      if (!is.null(conn) && DBI::dbIsValid(conn)) {
          if (verbose) message("Disconnecting from database...")
          DBI::dbDisconnect(conn)
          if (verbose) message("Database disconnected.")
      }
  }, add = TRUE)

  tryCatch({
      # Establish Connection
      conn <- tryCatch({
        if (verbose) message(sprintf("Connecting to database '%s' on %s:%d...", db_params$dbname, db_params$host, db_params$port))
        DBI::dbConnect(
          RPostgres::Postgres(),
          dbname = db_params$dbname,
          host = db_params$host,
          port = db_params$port,
          user = db_params$user,
          password = db_params$password
        )
      }, error = function(e) {
        stop("Database connection failed: ", e$message) # Stop execution if connection fails
      })

      if (is.null(conn) || !DBI::dbIsValid(conn)) {
        stop("Could not establish database connection.") # Should be caught by inner tryCatch, but double-check
      }
      if (verbose) message("Database connection successful.")

      # --- Helper: Check and Create Table (Define within this scope) ---
      check_and_create_table <- function(conn, table_name, schema_sql) {
         table_id <- DBI::Id(table = table_name)
         if (!DBI::dbExistsTable(conn, table_id)) {
            if (verbose) message(sprintf("Table '%s' does not exist. Creating...", table_name))
            tryCatch({
               DBI::dbExecute(conn, schema_sql)
               if (verbose) message(sprintf("Table '%s' created successfully.", table_name))
            }, error = function(e) {
               stop(sprintf("Failed to create table '%s': %s", table_name, e$message)) # Stop if table creation fails
            })
         } else {
            if (verbose) message(sprintf("Table '%s' already exists.", table_name))
         }
      }

      # --- Define and Create Table Schemas ---
      # (Schemas remain the same as before)
        # Locations Table
        locations_schema <- sprintf("
            CREATE TABLE %s (
            outbreak_id INTEGER PRIMARY KEY, report_id INTEGER, event_id INTEGER,
            longitude NUMERIC, latitude NUMERIC, geom GEOMETRY(Point, 4326),
            location VARCHAR(255), is_location_approx BOOLEAN, start_date DATE, end_date DATE,
            total_cases INTEGER, total_outbreaks INTEGER, oie_reference VARCHAR(100),
            national_reference VARCHAR(100), disease_name VARCHAR(255)
            );", table_names$locations)
        check_and_create_table(conn, table_names$locations, locations_schema)

        # Outbreak Table
        outbreak_schema <- sprintf("
            CREATE TABLE %s (
            outbreak_id INTEGER PRIMARY KEY, report_id INTEGER, area_id INTEGER,
            oieReference VARCHAR(100), nationalReference VARCHAR(100), latitude NUMERIC, longitude NUMERIC,
            location VARCHAR(255), is_location_approx BOOLEAN, isCluster BOOLEAN, clusterCount INTEGER,
            start_date TIMESTAMP WITH TIME ZONE, end_date TIMESTAMP WITH TIME ZONE, created_by_report_id INTEGER,
            last_update_report_id INTEGER, epi_unit_type_id INTEGER, epi_unit_type_key_value VARCHAR(100),
            epi_unit_type_translation VARCHAR(255), description_original TEXT, description_translation TEXT,
            disease_id INTEGER, disease_keyValue VARCHAR(255)
            );", table_names$outbreak)
        check_and_create_table(conn, table_names$outbreak, outbreak_schema) # Renamed PK/FKs here

        # Quantity Unit Table
        quantity_unit_schema <- sprintf("
            CREATE TABLE %s (
            outbreak_id INTEGER PRIMARY KEY, report_id INTEGER, id INTEGER,
            key_value VARCHAR(100), translation VARCHAR(255), description TEXT
            );", table_names$quantity_unit)
        check_and_create_table(conn, table_names$quantity_unit, quantity_unit_schema) # Renamed PK/FKs here

        # Admin Divisions Table
        admin_divisions_schema <- sprintf("
            CREATE TABLE %s (
            outbreak_id INTEGER, report_id INTEGER, area_id INTEGER, name VARCHAR(255),
            admin_level INTEGER, parent_area_id INTEGER, PRIMARY KEY (outbreak_id, area_id)
            );", table_names$admin_divisions)
        check_and_create_table(conn, table_names$admin_divisions, admin_divisions_schema)

        # Species Quantities Table
        species_quantities_schema <- sprintf("
            CREATE TABLE %s (
            outbreak_id INTEGER, report_id INTEGER, species_id INTEGER, species_name VARCHAR(255),
            is_wild BOOLEAN, susceptible INTEGER, cases INTEGER, deaths INTEGER, killed INTEGER,
            slaughtered INTEGER, vaccinated INTEGER, new_susceptible INTEGER, new_cases INTEGER,
            new_deaths INTEGER, new_killed INTEGER, new_slaughtered INTEGER, new_vaccinated INTEGER,
            species_type_wild_type VARCHAR(100), species_type_water_type VARCHAR(100),
            species_type_production_system VARCHAR(100), species_type_production_type_original VARCHAR(255),
            species_type_production_type_translation VARCHAR(255), created_by_current_report BOOLEAN,
            PRIMARY KEY (outbreak_id, species_id)
            );", table_names$species_quantities)
        check_and_create_table(conn, table_names$species_quantities, species_quantities_schema)

        # Control Measures Table (Assuming structure and renaming)
        control_measures_schema <- sprintf("
            CREATE TABLE %s (
            outbreak_id INTEGER, report_id INTEGER, measure_id INTEGER, measure_category VARCHAR(255),
            measure_name VARCHAR(255), measure_description TEXT, status_id INTEGER,
            status_key_value VARCHAR(100), status_translation VARCHAR(255),
            PRIMARY KEY (outbreak_id, measure_id)
            );", table_names$control_measures)
        # check_and_create_table(conn, table_names$control_measures, control_measures_schema) # Still requires unnesting logic in fetch
        if (verbose) message(sprintf("Skipping creation check for '%s' - requires unnesting logic during fetch.", table_names$control_measures))

        # Diagnostic Methods Table (Assuming structure and renaming)
        diagnostic_methods_schema <- sprintf("
            CREATE TABLE %s (
            outbreak_id INTEGER, report_id INTEGER, nature_id INTEGER, nature_key_value VARCHAR(100),
            nature_translation VARCHAR(255), nature_description TEXT,
            PRIMARY KEY (outbreak_id, nature_id)
            );", table_names$diagnostic_methods)
        check_and_create_table(conn, table_names$diagnostic_methods, diagnostic_methods_schema)

        # Add checks for additional_measures, measures_not_implemented if needed

      # --- Perform Writes within a Transaction ---
      if (verbose) message("Starting database write transaction...")
      if (verbose) message(head(locations))
      if (verbose) message(head(combined_locations_data))
      db_write_success <- tryCatch({
          DBI::dbWithTransaction(conn, {
              transaction_step_success <- TRUE # Track success within transaction

              # --- Write Helper Function (to avoid repetition) ---
              write_data_to_db <- function(data, target_table_name, pk_cols, conn, verbose) {
                  if (is.null(data) || nrow(data) == 0) {
                      if (verbose) message(sprintf("No data to write for %s.", target_table_name))
                      return(TRUE) # No data is not an error in this context
                  }
                  if (verbose) message(sprintf("Processing table: %s", target_table_name))

                  # Ensure PK columns exist and are not NA
                  if (!all(pk_cols %in% names(data))) {
                      warning(sprintf("One or more PK columns (%s) missing in combined data for '%s'. Skipping write.", paste(pk_cols, collapse=", "), target_table_name))
                      return(FALSE) # Consider this a failure within the transaction
                  }
                  data_valid_pk <- data %>% filter(dplyr::if_all(dplyr::all_of(pk_cols), ~ !is.na(.)))

                  if (nrow(data_valid_pk) == 0) {
                      if(verbose) message(sprintf("  No rows with valid primary keys found for %s.", target_table_name))
                      return(TRUE)
                  }

                  # Check existing IDs
                  # Build WHERE clause for composite keys if necessary
                  where_clause <- paste(sprintf("%s = $", pk_cols), seq_along(pk_cols), collapse = " AND ") # Placeholder for parameterized query
                  # This requires a more complex approach than simple ANY($1) for composite keys.
                  # For now, using the simpler (potentially less efficient) approach of checking first PK.
                  # A better approach might involve creating a temporary table or using paste/interaction.
                  first_pk <- pk_cols[1]
                  existing_ids_query <- sprintf("SELECT DISTINCT %s FROM %s WHERE %s = ANY($1)", first_pk, target_table_name, first_pk)
                  existing_ids <- tryCatch({
                      DBI::dbGetQuery(conn, existing_ids_query, params = list(unique(data_valid_pk[[first_pk]])))[[first_pk]]
                  }, error = function(e) {
                      warning(sprintf("Error checking existing IDs for %s: %s. Proceeding without check.", target_table_name, e$message))
                      # Depending on desired behavior, could return NULL, empty vector, or re-throw error
                      integer(0) # Assume none exist if check fails? Risky.
                  })


                  # Filter new data (simplistic check for first PK only)
                  new_data <- data_valid_pk %>% filter(!(.data[[first_pk]] %in% existing_ids))
                  # TODO: Implement proper composite key filtering if needed

                  if (nrow(new_data) > 0) {
                      if (verbose) message(sprintf("  Found %d potential new records to append.", nrow(new_data)))

                      # Special handling for locations (sf object)
                      if (target_table_name == table_names$locations) {
                          if (!all(c("longitude", "latitude") %in% names(new_data))) {
                              warning(sprintf("Longitude/Latitude columns missing in new location data for %s. Cannot create geometry.", target_table_name))
                              return(FALSE)
                          }
                          # Ensure sf package is loaded
                          if (!requireNamespace("sf", quietly = TRUE)) stop("Package 'sf' is required but not installed.")

                          # Filter rows with valid coordinates before creating sf object
                          new_data_coord_valid <- new_data %>% filter(!is.na(longitude) & !is.na(latitude))
                          if(nrow(new_data_coord_valid) < nrow(new_data) && verbose) {
                              message(sprintf("  Filtered out %d rows with missing coordinates before creating sf object.", nrow(new_data) - nrow(new_data_coord_valid)))
                          }

                          if(nrow(new_data_coord_valid) > 0) {
                              locations_sf <- sf::st_as_sf(new_data_coord_valid, coords = c("longitude", "latitude"), crs = 4326, remove = FALSE)
                              # Convert sf object back to data frame for DBI::dbWriteTable
                              data_to_write <- as.data.frame(locations_sf)
                              # DEBUG PRINT for locations
                              if (verbose) {
                                  message("  DEBUG: Head of locations_df before writing:")
                                  print(utils::head(data_to_write))
                              }
                          } else {
                              if(verbose) message("  No new location records with valid coordinates found.")
                              return(TRUE) # No valid data to write is not an error
                          }

                      } else {
                          data_to_write <- new_data
                      }

                      if (verbose) message(sprintf("  Writing %d records to %s...", nrow(data_to_write), target_table_name))
                      DBI::dbWriteTable(conn, name = target_table_name, value = data_to_write, append = TRUE, row.names = FALSE)
                      if (verbose) message(sprintf("  Appended %d records to %s.", nrow(data_to_write), target_table_name))
                  } else {
                      if (verbose) message(sprintf("  No new records to append for %s based on %s.", target_table_name, first_pk))
                  }
                  return(TRUE) # Indicate success for this table

              } # End write_data_to_db function definition

              # --- Write Details Tables ---
              if (!is.null(combined_details_list)) {
                  details_write_results <- purrr::imap_lgl(combined_details_list, function(data, element_name) {
                      target_table <- NULL
                      pk_cols <- NULL
                      current_data_renamed <- data # Start with combined data

                      # Define mappings and rename columns (match the CREATE TABLE schemas)
                      if (element_name == "outbreak") {
                          target_table <- table_names$outbreak
                          # Rename only if columns exist to avoid errors if fetch failed partially
                          cols_to_rename <- intersect(names(current_data_renamed), c("outbreakId", "reportId", "areaId", "isLocationApprox", "clusterCount", "startDate", "endDate", "createdByReportId", "lastUpdateReportId", "epiUnitType_id", "epiUnitType_keyValue", "epiUnitType_translation", "description_original", "description_translation"))
                          if(length(cols_to_rename) > 0) {
                              current_data_renamed <- current_data_renamed %>%
                                  rename(
                                      outbreak_id = outbreakId, report_id = reportId, area_id = areaId,
                                      is_location_approx = isLocationApprox, cluster_count = clusterCount,
                                      start_date = startDate, end_date = endDate, created_by_report_id = createdByReportId,
                                      last_update_report_id = lastUpdateReportId, epi_unit_type_id = epiUnitType_id,
                                      epi_unit_type_key_value = epiUnitType_keyValue, epi_unit_type_translation = epiUnitType_translation,
                                      description_original = description_original, description_translation = description_translation
                                  )
                          }
                          pk_cols <- "outbreak_id"
                      } else if (element_name == "quantityUnit") {
                          target_table <- table_names$quantity_unit
                          cols_to_rename <- intersect(names(current_data_renamed), c("outbreakId", "reportId", "keyValue"))
                           if(length(cols_to_rename) > 0) {
                               current_data_renamed <- current_data_renamed %>% rename(outbreak_id = outbreakId, report_id = reportId, key_value = keyValue)
                           }
                          pk_cols <- "outbreak_id"
                      } else if (element_name == "adminDivisions") {
                          target_table <- table_names$admin_divisions
                          cols_to_rename <- intersect(names(current_data_renamed), c("outbreakId", "reportId", "areaId", "adminLevel", "parentAreaId"))
                           if(length(cols_to_rename) > 0) {
                               current_data_renamed <- current_data_renamed %>% rename(outbreak_id = outbreakId, report_id = reportId, area_id = areaId, admin_level = adminLevel, parent_area_id = parentAreaId)
                           }
                          pk_cols <- c("outbreak_id", "area_id")
                      } else if (element_name == "speciesQuantities") {
                          target_table <- table_names$species_quantities
                          cols_to_rename <- intersect(names(current_data_renamed), c("outbreakId", "reportId", "speciesId", "speciesName", "isWild", "createdByCurrentReport"))
                           if(length(cols_to_rename) > 0) {
                               current_data_renamed <- current_data_renamed %>% rename(outbreak_id = outbreakId, report_id = reportId, species_id = speciesId, species_name = speciesName, is_wild = isWild, created_by_current_report = createdByCurrentReport)
                           }
                          pk_cols <- c("outbreak_id", "species_id")
                      } else if (element_name == "controlMeasures") {
                          target_table <- table_names$control_measures
                          # Add renaming if schema is confirmed and table created
                          pk_cols <- c("outbreak_id", "measure_id") # Assuming schema
                      } else if (element_name == "diagnosticMethods") {
                          target_table <- table_names$diagnostic_methods
                          cols_to_rename <- intersect(names(current_data_renamed), c("outbreakId", "reportId", "nature_id", "nature_keyValue", "nature_translation", "nature_description"))
                           if(length(cols_to_rename) > 0) {
                               current_data_renamed <- current_data_renamed %>% rename(outbreak_id = outbreakId, report_id = reportId, nature_id = nature_id, nature_key_value = nature_keyValue, nature_translation = nature_translation, nature_description = nature_description)
                           }
                          pk_cols <- c("outbreak_id", "nature_id")
                      }
                      # Add mappings for additional_measures, measures_not_implemented if needed

                      if (is.null(target_table) || is.null(pk_cols)) {
                          if(verbose) message(sprintf("Skipping element '%s' - no table mapping or PK defined.", element_name))
                          return(TRUE) # Skip is not an error
                      }

                      # Check if table exists before writing (important for optional/unnested tables)
                      if (!DBI::dbExistsTable(conn, DBI::Id(table = target_table))) {
                           if(verbose) message(sprintf("Skipping write for '%s' - target table does not exist.", target_table))
                           return(TRUE) # Skip is not an error
                      }

                      # Call the write helper
                      write_success <- write_data_to_db(current_data_renamed, target_table, pk_cols, conn, verbose)
                      return(write_success)
                  })
                  # Check if all detail writes were successful
                  if (!all(details_write_results)) {
                      stop("One or more errors occurred writing detail tables. Rolling back transaction.")
                  }
              } else {
                   if(verbose) message("No combined details data to write.")
              }


              # --- Write Locations Table ---
              if (!is.null(combined_locations_data)) {
                  # Rename columns here before passing to helper
                  cols_to_rename_loc <- intersect(names(combined_locations_data), c("outbreakId", "reportId", "eventId", "isLocationApprox", "startDate", "endDate", "totalCases", "totalOutbreaks", "oieReference", "nationalReference", "disease"))
                  if(length(cols_to_rename_loc) > 0) {
                      locations_renamed <- combined_locations_data %>%
                          rename(
                              outbreak_id = outbreakId, report_id = reportId, event_id = eventId,
                              is_location_approx = isLocationApprox, start_date = startDate, end_date = endDate,
                              total_cases = totalCases, total_outbreaks = totalOutbreaks,
                              oie_reference = oieReference, national_reference = nationalReference,
                              disease_name = disease
                          )
                  } else {
                      locations_renamed <- combined_locations_data # No columns to rename?
                  }

                  locations_write_success <- write_data_to_db(
                      data = locations_renamed,
                      target_table_name = table_names$locations,
                      pk_cols = "outbreak_id",
                      conn = conn,
                      verbose = verbose
                  )
                  if (!locations_write_success) {
                      stop("Error occurred writing locations table. Rolling back transaction.")
                  }
              } else {
                   if(verbose) message("No combined location data to write.")
              }

              # If we reach here, all writes within the transaction were successful (or skipped appropriately)
              return(TRUE) # Commit transaction
          }) # End dbWithTransaction
          return(TRUE) # Return TRUE if transaction committed
      }, error = function(e) {
          # This catches errors during the transaction (e.g., write failures causing stop(), connection issues)
          warning("Database write transaction failed: ", e$message)
          return(FALSE) # Indicate transaction rollback / failure
      }) # End tryCatch for dbWithTransaction

  }, error = function(e) {
      # This catches errors *before* or *during* the transaction block setup (e.g., connection, table creation)
      warning("Error during database operations phase: ", e$message)
      db_write_success <<- FALSE # Ensure flag is FALSE
  }) # End outer tryCatch for DB operations phase


  # --- Final Message ---
  if (verbose) message("\n--- Database Update Process Finished ---")
  final_success <- db_write_success && !any_fetch_error # Overall success requires successful fetch AND write

  if (!final_success) {
    message("Process completed with errors or warnings. Please review messages.")
    if (any_fetch_error) message("  - Errors occurred during data fetching.")
    if (!db_write_success) message("  - Errors occurred during database writing.")
  } else {
    if (verbose) message("Process completed successfully.")
  }

  return(invisible(final_success))

}
