#' Update WOAH Outbreak Data in a PostgreSQL/PostGIS Database
#'
#' Fetches outbreak data (locations and full details) from the WOAH API for a
#' specified date range, potentially in batches, and upserts it into
#' corresponding tables in a PostgreSQL database with the PostGIS extension.
#' Tables are created if they don't exist.
#'
#' @details
#' The function performs the following steps:
#' \enumerate{
#'   \item Connects to the specified PostgreSQL database.
#'   \item Defines target table names (prefixed with `table_prefix`).
#'   \item Checks if each target table exists. If not, it creates the table with
#'         appropriate columns and data types. The locations table includes a
#'         PostGIS geometry column. Primary keys are defined to prevent duplicates.
#'   \item Splits the date range into smaller intervals if necessary (using `batch_interval`).
#'   \item For each date interval:
#'     \itemize{
#'       \item Fetches location data using `rwahis::get_woah_outbreak_locations`.
#'       \item Fetches full outbreak details using `rwahis::get_woah_outbreaks_full_info`.
#'       \item Processes and prepares data for insertion.
#'       \item For each data table (locations, outbreak, speciesQuantities, etc.):
#'         \itemize{
#'           \item Identifies new records based on primary keys (existing records are skipped).
#'           \item Appends only the new records to the corresponding database table.
#'           \item Location data is converted to `sf` objects before writing.
#'         }
#'     }
#'   \item Disconnects from the database.
#' }
#'
#' **Required Database Setup:**
#' \itemize{
#'   \item PostgreSQL server.
#'   \item PostGIS extension enabled (`CREATE EXTENSION postgis;`).
#'   \item User specified in `db_params` needs connection and table creation/write permissions.
#' }
#'
#' **Table Schema Notes:**
#' \itemize{
#'  \item `locations`: Contains outbreak locations with a PostGIS point geometry (SRID 4326). PK: `outbreakId`.
#'  \item `outbreak`: Core outbreak details. PK: `outbreakId`.
#'  \item `quantity_unit`: Details about the quantity unit used. PK: `outbreakId`.
#'  \item `admin_divisions`: Administrative divisions associated with outbreaks. PK: `outbreakId`, `areaId`.
#'  \item `species_quantities`: Species-specific numbers (susceptible, cases, etc.). PK: `outbreakId`, `speciesId`.
#'  \item `control_measures`: Control measures applied. PK: `outbreakId`, `measure_id`.
#'  \item `diagnostic_methods`: Diagnostic methods used. PK: `outbreakId`, `nature_id`.
#'  \item `additional_measures`: (Currently often empty) PK: `outbreakId`, `measure_id` (assuming structure).
#'  \item `measures_not_implemented`: (Currently often empty) PK: `outbreakId`, `measure_id` (assuming structure).
#' }
#' Column types are inferred but might need adjustment based on specific data variations.
#' Character varying lengths are set generously.
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
#' @param table_prefix Character string prefix for all created/updated database tables. Default is "woah_".
#' @param language Language code for API requests (default: "en").
#' @param verbose Logical: Print progress messages? (Default: TRUE for this function).
#'
#' @return Invisibly returns TRUE if completed (potentially with warnings), or FALSE if a critical error occurred.
#'
#' @importFrom DBI dbConnect dbDisconnect dbExistsTable dbExecute dbGetQuery dbWriteTable Id
#' @importFrom RPostgres Postgres
#' @importFrom sf st_as_sf st_write st_geometry st_crs<- st_crs
#' @importFrom lubridate ymd interval duration floor_date %m+% months days weeks
#' @importFrom dplyr %>% filter select distinct anti_join mutate across all_of rename relocate if_else bind_rows
#' @importFrom purrr map map_chr safely walk imap list_rbind set_names pluck keep
#' @importFrom rlang := abort
#' @importFrom methods is
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

  # --- Database Connection ---
  conn <- NULL # Initialize conn outside tryCatch
  on.exit(if (!is.null(conn) && DBI::dbIsValid(conn)) {
            if (verbose) message("Disconnecting from database...")
            DBI::dbDisconnect(conn)
            if (verbose) message("Database disconnected.")
          }, add = TRUE)

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
    message("Database connection failed: ", e$message)
    return(NULL)
  })

  if (is.null(conn) || !DBI::dbIsValid(conn)) {
    message("Could not establish database connection. Aborting.")
    return(invisible(FALSE))
  }
  if (verbose) message("Database connection successful.")

  # --- Define Table Names ---
  table_names <- list(
    locations = paste0(table_prefix, "locations"),
    outbreak = paste0(table_prefix, "outbreak"),
    quantity_unit = paste0(table_prefix, "quantity_unit"),
    admin_divisions = paste0(table_prefix, "admin_divisions"),
    species_quantities = paste0(table_prefix, "species_quantities"),
    control_measures = paste0(table_prefix, "control_measures"),
    diagnostic_methods = paste0(table_prefix, "diagnostic_methods"),
    additional_measures = paste0(table_prefix, "additional_measures"),
    measures_not_implemented = paste0(table_prefix, "measures_not_implemented")
  )

  # --- Helper: Check and Create Table ---
  # (This needs to be implemented based on expected columns and types)
  check_and_create_table <- function(conn, table_name, schema_sql) {
     table_id <- DBI::Id(table = table_name) # Use Id for schema qualification if needed later
     if (!DBI::dbExistsTable(conn, table_id)) {
        if (verbose) message(sprintf("Table '%s' does not exist. Creating...", table_name))
        tryCatch({
           DBI::dbExecute(conn, schema_sql)
           if (verbose) message(sprintf("Table '%s' created successfully.", table_name))
        }, error = function(e) {
           stop(sprintf("Failed to create table '%s': %s", table_name, e$message))
        })
     } else {
        if (verbose) message(sprintf("Table '%s' already exists.", table_name))
     }
  }

  # --- Define and Create Table Schemas (Simplified Example - Needs Refinement) ---
  # NOTE: These schemas are basic examples. Column types (esp. VARCHAR lengths)
  # and constraints (NULL/NOT NULL, primary/foreign keys) should be carefully
  # reviewed and adjusted based on actual data and database design needs.

  # Locations Table
  locations_schema <- sprintf("
    CREATE TABLE %s (
      outbreak_id INTEGER PRIMARY KEY,
      report_id INTEGER,
      event_id INTEGER,
      longitude NUMERIC,
      latitude NUMERIC,
      geom GEOMETRY(Point, 4326), -- PostGIS geometry column
      location VARCHAR(255),
      is_location_approx BOOLEAN,
      start_date DATE,
      end_date DATE,
      total_cases INTEGER,
      total_outbreaks INTEGER,
      oie_reference VARCHAR(100),
      national_reference VARCHAR(100),
      disease_name VARCHAR(255)
      -- Add other relevant columns from get_woah_outbreak_locations if needed
    );", table_names$locations)
  check_and_create_table(conn, table_names$locations, locations_schema)

  # Outbreak Table
  outbreak_schema <- sprintf("
    CREATE TABLE %s (
      outbreakId INTEGER PRIMARY KEY,
      reportId INTEGER,
      areaId INTEGER,
      oieReference VARCHAR(100),
      nationalReference VARCHAR(100),
      latitude NUMERIC,
      longitude NUMERIC,
      location VARCHAR(255),
      isLocationApprox BOOLEAN,
      isCluster BOOLEAN,
      clusterCount INTEGER,
      startDate TIMESTAMP WITH TIME ZONE, -- API returns timestamp
      endDate TIMESTAMP WITH TIME ZONE,
      createdByReportId INTEGER,
      lastUpdateReportId INTEGER,
      epiUnitType_id INTEGER,
      epiUnitType_keyValue VARCHAR(100),
      epiUnitType_translation VARCHAR(255),
      description_original TEXT,
      description_translation TEXT,
      disease_id INTEGER, -- Assuming disease might be nested
      disease_keyValue VARCHAR(255)
      -- Add other relevant columns
    );", table_names$outbreak)
   check_and_create_table(conn, table_names$outbreak, outbreak_schema)

   # Quantity Unit Table
   quantity_unit_schema <- sprintf("
    CREATE TABLE %s (
      outbreakId INTEGER PRIMARY KEY, -- Assuming one unit per outbreak? Check API docs.
      reportId INTEGER,
      id INTEGER,
      keyValue VARCHAR(100),
      translation VARCHAR(255),
      description TEXT
    );", table_names$quantity_unit)
   check_and_create_table(conn, table_names$quantity_unit, quantity_unit_schema)

  # Admin Divisions Table
  admin_divisions_schema <- sprintf("
    CREATE TABLE %s (
      outbreak_id INTEGER,
      report_id INTEGER,
      area_id INTEGER,
      name VARCHAR(255),
      admin_level INTEGER,
      parent_area_id INTEGER,
      PRIMARY KEY (outbreak_id, area_id) -- Composite key
    );", table_names$admin_divisions)
   check_and_create_table(conn, table_names$admin_divisions, admin_divisions_schema)

   # Species Quantities Table (Complex - needs careful column definition)
   species_quantities_schema <- sprintf("
    CREATE TABLE %s (
      outbreak_id INTEGER,
      report_id INTEGER,
      species_id INTEGER,
      species_name VARCHAR(255),
      is_wild BOOLEAN,
      susceptible INTEGER,
      cases INTEGER,
      deaths INTEGER,
      killed INTEGER,
      slaughtered INTEGER,
      vaccinated INTEGER,
      new_susceptible INTEGER,
      new_cases INTEGER,
      new_deaths INTEGER,
      new_killed INTEGER,
      new_slaughtered INTEGER,
      new_vaccinated INTEGER,
      species_type_wild_type VARCHAR(100),
      species_type_water_type VARCHAR(100),
      species_type_production_system VARCHAR(100),
      species_type_production_type_original VARCHAR(255),
      species_type_production_type_translation VARCHAR(255),
      created_by_current_report BOOLEAN,
      PRIMARY KEY (outbreak_id, species_id)
    );", table_names$species_quantities)
   check_and_create_table(conn, table_names$species_quantities, species_quantities_schema)

   # Control Measures Table
   control_measures_schema <- sprintf("
    CREATE TABLE %s (
       outbreakId INTEGER,
       reportId INTEGER,
       measure_id INTEGER, -- Assuming 'id' inside 'measure' after unnest
       measure_category VARCHAR(255),
       measure_name VARCHAR(255),
       measure_description TEXT,
       status_id INTEGER,
       status_keyValue VARCHAR(100),
       status_translation VARCHAR(255),
       PRIMARY KEY (outbreakId, measure_id) -- Composite key
    );", table_names$control_measures)
    # Need to implement unnesting for measure and status inside get_woah_outbreak_details first
    # check_and_create_table(conn, table_names$control_measures, control_measures_schema)
    if (verbose) message(sprintf("Skipping creation check for '%s' - requires unnesting logic.", table_names$control_measures))


   # Diagnostic Methods Table
   diagnostic_methods_schema <- sprintf("
    CREATE TABLE %s (
       outbreakId INTEGER,
       reportId INTEGER,
       nature_id INTEGER,
       nature_keyValue VARCHAR(100),
       nature_translation VARCHAR(255),
       nature_description TEXT,
       PRIMARY KEY (outbreakId, nature_id) -- Composite key (assuming nature_id unique per outbreak)
    );", table_names$diagnostic_methods)
   check_and_create_table(conn, table_names$diagnostic_methods, diagnostic_methods_schema)

   # Add schemas for additional_measures and measures_not_implemented if their structure is known
   # ...

  # --- Date Batching ---
  if (!is.null(batch_interval)) {
    interval_parts <- strsplit(batch_interval, " ")[[1]]
    if (length(interval_parts) != 2 || !interval_parts[1] %in% c("1", "2", "3", "4", "6") || !interval_parts[2] %in% c("day", "days", "week", "weeks", "month", "months")) {
        warning("Invalid batch_interval format. Using '1 month'. Supported: N days/weeks/months (N=1-6).")
        batch_interval <- "1 month"
        interval_parts <- c("1", "month")
    }
    num <- as.numeric(interval_parts[1])
    unit <- interval_parts[2]

    # Create sequence of start dates
    start_dates <- seq(start_date, end_date, by = batch_interval)
    # Create corresponding end dates (handle end of sequence)
# Convert to Period objects for proper arithmetic
end_dates <- (start_dates + 
             if(grepl("month", unit)) months(num) else months(0) + 
             if(grepl("week", unit)) weeks(num) else weeks(0) + 
             if(grepl("day", unit)) days(num) else days(0)) - days(1)
    # Ensure last end date doesn't exceed overall end_date
    end_dates[length(end_dates)] <- min(end_dates[length(end_dates)], end_date)
    # Ensure start dates don't exceed end date
    valid_indices <- start_dates <= end_date
    start_dates <- start_dates[valid_indices]
    end_dates <- end_dates[valid_indices]

    date_intervals <- purrr::map2(start_dates, end_dates, ~list(start = .x, end = .y))
    if (verbose) message(sprintf("Processing date range in %d batches of interval '%s'.", length(date_intervals), batch_interval))
  } else {
    date_intervals <- list(list(start = start_date, end = end_date))
    if (verbose) message("Processing entire date range at once.")
  }

  # --- Process Batches ---
  overall_success <- TRUE
  processed_outbreak_ids <- integer(0) # Keep track across batches if needed, though upsert handles it per batch

  for (i in seq_along(date_intervals)) {
    interval <- date_intervals[[i]]
    int_start <- interval$start
    int_end <- interval$end
    if (verbose) message(sprintf("\n--- Processing Batch %d/%d (%s to %s) ---", i, length(date_intervals), int_start, int_end))

    # --- Fetch Data for Interval ---
    locations_data <- NULL
    details_list <- NULL
    fetch_error <- FALSE

    tryCatch({
        if (verbose) {
            message(sprintf("\nFetching locations for %s to %s...", int_start, int_end))
            if (!is.null(disease_name)) {
                message(sprintf("  Disease filter: %s", disease_name))
            }
        }
        locations_data <- get_woah_outbreak_locations(
            start_date = int_start,
            end_date = int_end,
            disease_name = disease_name,
            language = language,
            verbose = verbose # Pass through verbose setting
        )

        if (is.null(locations_data)) {
            warning(sprintf("Failed to fetch locations for interval %s - %s. Skipping batch.", int_start, int_end))
            fetch_error <- TRUE
        } else if (nrow(locations_data) == 0) {
            if (verbose) message("No locations found for this interval.")
            # Continue to next batch if no locations found
            next
        } else {
             if (verbose) message(sprintf("Fetched %d location records.", nrow(locations_data)))
             if (verbose) message("\nFetching full outbreak details...")
             if (verbose) message(sprintf("Fetched %d location records.", head(locations_data)))
             print(head(locations_data))
             details_list <- get_woah_outbreaks_full_info(
                 start_date = int_start,
                 end_date = int_end,
                 disease_name = disease_name,
                 language = language,
                 verbose = verbose # Pass through verbose setting
             )
             if (verbose && !is.null(details_list)) {
                 total_outbreaks <- sum(sapply(details_list, function(x) if(is.data.frame(x)) nrow(x) else 0))
                 message(sprintf("  Retrieved details with %d total records across all tables", total_outbreaks))
             }
             if (is.null(details_list)) {
                 warning(sprintf("Failed to fetch full details for interval %s - %s despite finding locations. Skipping batch.", int_start, int_end))
                 fetch_error = TRUE
             } else if (length(details_list) == 0) {
                 if (verbose) message("No outbreak details found for this interval.")
                 next
             } else {
                 if (verbose) message(sprintf("Fetched details, including %d outbreak records.", nrow(details_list$outbreak)))
             }
        }
    }, error = function(e) {
        warning(sprintf("Error fetching data for interval %s - %s: %s. Skipping batch.", int_start, int_end, e$message))
        fetch_error <<- TRUE # Assign to outer scope
    })

    if (fetch_error || is.null(details_list) || is.null(locations_data)) {
        overall_success <- FALSE # Mark failure if fetch error occurred
        next # Skip to next batch
    }

    # --- Process and Write Data ---
    batch_success <- TRUE # Track success within the batch writing phase

    # 2. Process and Write Other Tables from details_list
    purrr::walk(names(details_list), function(element_name) {
        if (!batch_success) return() # Skip if previous step failed

        # Map element name to table name and define primary key(s)
        target_table <- NULL
        pk_cols <- NULL
        current_data <- details_list[[element_name]]

        # Basic check
        if (is.null(current_data) || !is.data.frame(current_data) || nrow(current_data) == 0) {
            if(verbose) message(sprintf("Skipping empty or invalid data for element: %s", element_name))
            return()
        }

            # --- Define Table Mapping and PKs ---
            # This mapping needs to be robust and match the CREATE TABLE statements
            if (element_name == "outbreak") {
                target_table <- table_names$outbreak
                current_data <- current_data %>%
                    rename(
                        outbreak_id = outbreakId,
                        report_id = reportId,
                        area_id = areaId,
                        is_location_approx = isLocationApprox,
                        cluster_count = clusterCount,
                        start_date = startDate,
                        end_date = endDate,
                        created_by_report_id = createdByReportId,
                        last_update_report_id = lastUpdateReportId,
                        epi_unit_type_id = epiUnitType_id,
                        epi_unit_type_key_value = epiUnitType_keyValue,
                        epi_unit_type_translation = epiUnitType_translation,
                        description_original = description_original,
                        description_translation = description_translation
                    )
                pk_cols <- "outbreak_id"
            } else if (element_name == "quantityUnit") {
                target_table <- table_names$quantity_unit
                current_data <- current_data %>%
                    rename(
                        outbreak_id = outbreakId,
                        report_id = reportId,
                        key_value = keyValue
                    )
                pk_cols <- "outbreak_id"
            } else if (element_name == "adminDivisions") {
                target_table <- table_names$admin_divisions
                current_data <- current_data %>%
                    rename(
                        outbreak_id = outbreakId,
                        report_id = reportId,
                        area_id = areaId,
                        admin_level = adminLevel,
                        parent_area_id = parentAreaId
                    )
                pk_cols <- c("outbreak_id", "area_id")
            } else if (element_name == "speciesQuantities") {
                target_table <- table_names$species_quantities
                current_data <- current_data %>%
                    rename(
                        outbreak_id = outbreakId,
                        report_id = reportId,
                        species_id = speciesId,
                        species_name = speciesName,
                        is_wild = isWild,
                        created_by_current_report = createdByCurrentReport
                    )
                pk_cols <- c("outbreak_id", "species_id")
            } else if (element_name == "controlMeasures") {
                target_table <- table_names$control_measures
                current_data <- current_data %>%
                    rename(
                        outbreak_id = outbreakId,
                        report_id = reportId,
                        measure_id = measure_id,
                        measure_category = measure_category,
                        measure_name = measure_name,
                        measure_description = measure_description,
                        status_id = status_id,
                        status_key_value = status_keyValue,
                        status_translation = status_translation
                    )
                pk_cols <- c("outbreak_id", "measure_id")
            } else if (element_name == "diagnosticMethods") {
                target_table <- table_names$diagnostic_methods
                current_data <- current_data %>%
                    rename(
                        outbreak_id = outbreakId,
                        report_id = reportId,
                        nature_id = nature_id,
                        nature_key_value = nature_keyValue,
                        nature_translation = nature_translation,
                        nature_description = nature_description
                    )
                pk_cols <- c("outbreak_id", "nature_id")
            }
        # Add mappings for additional_measures, measures_not_implemented if needed

        if (is.null(target_table) || is.null(pk_cols)) {
            if(verbose) message(sprintf("Skipping element '%s' - no table mapping or PK defined.", element_name))
            return()
        }

        # --- Write Logic (Append New Only) ---
        tryCatch({
            if (verbose) message(sprintf("Processing table: %s", target_table))

            # Ensure PK columns exist and are not NA
            if (!all(pk_cols %in% names(current_data))) {
                warning(sprintf("One or more PK columns (%s) missing in data for '%s'. Skipping write.", paste(pk_cols, collapse=", "), target_table))
                return()
            }
            data_valid_pk <- current_data %>% filter(dplyr::if_all(all_of(pk_cols), ~ !is.na(.)))

            if (nrow(data_valid_pk) == 0) {
                 if(verbose) message(sprintf("  No rows with valid primary keys found for %s.", target_table))
                 return()
            }

            # Check existing IDs (more complex for composite keys)
            # For simplicity, let's check based on the first PK column for now
            # A robust solution would use interaction() or paste() for composite keys
            first_pk <- pk_cols[1]
            existing_ids_query <- sprintf("SELECT DISTINCT %s FROM %s WHERE %s = ANY($1)", first_pk, target_table, first_pk)
            existing_ids <- DBI::dbGetQuery(conn, existing_ids_query, params = list(unique(data_valid_pk[[first_pk]])))[[first_pk]]

            # Filter new data (simplistic check - might miss updates or true composite duplicates)
            new_data <- data_valid_pk %>% filter(!(.data[[first_pk]] %in% existing_ids))

            # TODO: Implement proper composite key checking if needed

            if (nrow(new_data) > 0) {
                if (verbose) message(sprintf("  Found %d potential new records to append.", nrow(new_data)))
                # Ensure column names match DB (might need renaming/selection)
                # For now, assume column names are compatible after processing
                if (verbose) message(sprintf("  Writing %d records to %s...", nrow(new_data), target_table))
                DBI::dbWriteTable(conn, name = target_table, value = new_data, append = TRUE, row.names = FALSE)
                if (verbose) message(sprintf("  Appended %d records to %s.", nrow(new_data), target_table))
            } else {
                if (verbose) message(sprintf("  No new records to append for %s based on %s.", target_table, first_pk))
            }

        }, error = function(e) {
            warning(sprintf("Error processing/writing table '%s': %s", target_table, e$message))
            batch_success <<- FALSE
        })

    }) # End walk through details_list elements

  # 1. Process and Write Locations
            tryCatch({
                if (nrow(locations_data) > 0) {
                    target_table <- table_names$locations
                    pk_col <- "outbreakId" # Original PK name before rename
                    if (verbose) message(sprintf("Processing table: %s", target_table))

                    # Ensure required columns exist
                    if (!all(c("longitude", "latitude", pk_col) %in% names(locations_data))) {
                         warning(sprintf("Required columns ('longitude', 'latitude', '%s') missing in locations data for this batch. Skipping write.", pk_col))
                    } else {
                        # Rename columns to match database schema
                        locations_valid <- locations_data %>%
                            rename(
                                outbreak_id = outbreakId,
                                report_id = reportId,
                                event_id = eventId,
                                is_location_approx = isLocationApprox,
                                start_date = startDate,
                                end_date = endDate,
                                total_cases = totalCases,
                                total_outbreaks = totalOutbreaks,
                                oie_reference = oieReference,
                                national_reference = nationalReference,
                                disease_name = disease
                            ) %>%
                            filter(!is.na(longitude), !is.na(latitude), !is.na(outbreak_id))

                        if (nrow(locations_valid) > 0) {
                            # Check existing IDs using outbreak_id with proper error handling
                            existing_ids <- tryCatch({
                                query <- sprintf("SELECT outbreak_id FROM %s WHERE outbreak_id = ANY($1)", target_table)
                                result <- DBI::dbGetQuery(conn, query, params = list(unique(locations_valid$outbreak_id)))
                                if (nrow(result) > 0) result$outbreak_id else integer(0)
                            }, error = function(e) {
                                if (verbose) message(sprintf("Error checking existing IDs: %s", e$message))
                                integer(0)
                            })

                            # Use the renamed primary key column 'outbreak_id' for filtering
                            new_locations <- locations_valid %>% filter(!(.data[["outbreak_id"]] %in% existing_ids))

                            if (nrow(new_locations) > 0) {
                                if (verbose) {
                                    message(sprintf("  Found %d new location records to append.", nrow(new_locations)))
                                    # message("  First few rows of locations data:") # Print before sf conversion might be less useful
                                    # print(head(new_locations))
                                }
                                # Convert to sf object
                                locations_sf <- sf::st_as_sf(new_locations, coords = c("longitude", "latitude"), crs = 4326, remove = FALSE)

                                 # Write using DBI with explicit transaction handling
                                 tryCatch({
                                    DBI::dbWithTransaction(conn, {
                                      # Ensure sf package is loaded
                                      if (!requireNamespace("sf", quietly = TRUE)) {
                                        stop("Package 'sf' is required but not installed.")
                                      }
                                      # Convert sf object to data frame
                                      locations_df <- as.data.frame(locations_sf)
                                      # --- DEBUG PRINT ---
                                      if (verbose) {
                                          message("  DEBUG: Head of locations_df before writing:")
                                          print(utils::head(locations_df))
                                      }
                                      # --- END DEBUG PRINT ---
                                      if (verbose) message(sprintf("  Writing %d new records to %s...", nrow(locations_df), target_table)) # Use nrow(locations_df)
                                      DBI::dbWriteTable(
                                        conn,
                                        name = target_table,
                                        value = locations_df,
                                        append = TRUE,
                                        row.names = FALSE
                                      )
                                      if (verbose) message(sprintf("  Appended %d new records to %s.", nrow(new_locations), target_table))
                                    })
                                  }, error = function(e) {
                                    warning(sprintf("Failed to write to %s: %s", target_table, e$message))
                                    if (verbose) message("Attempting to write records individually...")

                                    # Fallback to writing records one by one
                                    success_count <- 0
                                    for (i in seq_len(nrow(locations_sf))) {
                                      tryCatch({
                                        # Convert sf object to data frame for each record
                                        location_df <- as.data.frame(locations_sf[i,])
                                        if (verbose) message(sprintf("  Writing record %d to %s...", i, target_table))
                                        DBI::dbWriteTable(
                                          conn,
                                          name = target_table,
                                          value = location_df,
                                          append = TRUE,
                                          row.names = FALSE
                                        )
                                        if (verbose) message(sprintf("  Successfully wrote record %d to %s.", i, target_table))
                                        success_count <- success_count + 1
                                      }, error = function(e) {
                                        # Correctly handle error when writing individual record
                                        if (verbose) message(sprintf("  Failed to write record %d individually: %s", i, e$message))
                                        # Indicate failure for this record, but allow loop to continue
                                      }) # Close tryCatch for individual write
                                    } # Close for loop
                                    # If fallback was attempted, report how many succeeded
                                    if (verbose) message(sprintf("  Fallback write attempt finished. %d out of %d records successfully written individually.", success_count, nrow(locations_sf)))
                                    # Ensure the overall batch is marked as failed if the bulk write failed
                                    batch_success <<- FALSE
                                  }) # Close tryCatch for bulk write

                            } else {
                                if (verbose) message(sprintf("  No new location records to append based on outbreak_id."))
                            }
                        } else {
                             if(verbose) message("  No valid location rows with coordinates and outbreak_id found.")
                        }
                    }
                } else {
                     if(verbose) message("  No location data fetched for this batch.")
                }
            }, error = function(e) {
                 warning(sprintf("Error processing/writing table '%s': %s", table_names$locations, e$message))
                 batch_success <<- FALSE
            }) # End tryCatch for locations processing/writing

    # --- Batch Completion ---
    if (!batch_success) {
        warning(sprintf("One or more errors occurred during database write for batch %s - %s.", int_start, int_end))
        overall_success <- FALSE # Mark overall failure if any batch fails
    }

  } # End loop through date_intervals

  # --- Final Message ---
  if (verbose) message("\n--- Database Update Process Finished ---")
  if (!overall_success) {
    message("Process completed with errors or warnings. Please review messages.")
  } else {
    if (verbose) message("Process completed successfully.")
  }

  return(invisible(overall_success))

}
