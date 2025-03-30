\name{update_woah_db}
\alias{update_woah_db}
\title{Update WOAH Outbreak Data in a PostgreSQL/PostGIS Database}
\description{
Fetches outbreak data (locations and full details) from the WOAH API for a specified date range and upserts it into corresponding tables in a PostgreSQL database with PostGIS extension.
}
\usage{
update_woah_db(
  start_date,
  end_date,
  disease_name = NULL,
  db_params,
  batch_interval = "1 month",
  table_prefix = "woah_",
  language = "en",
  verbose = TRUE
)
}
\arguments{
\item{start_date}{Character string or Date object for the start date (YYYY-MM-DD)}
\item{end_date}{Character string or Date object for the end date (YYYY-MM-DD)}
\item{disease_name}{Optional disease name to filter by (NULL for all diseases)}
\item{db_params}{List containing database connection parameters (dbname, host, port, user, password)}
\item{batch_interval}{Interval for batch processing (e.g., "1 month", "2 weeks")}
\item{table_prefix}{Prefix for database tables (default: "woah_")}
\item{language}{Language code for API requests (default: "en")}
\item{verbose}{Logical: Print progress messages? (default: TRUE)}
}
\value{
Invisibly returns TRUE if successful, FALSE if errors occurred
}
\examples{
\dontrun{
db_params <- list(
  dbname = "animal_health_db",
  host = "localhost",
  port = 5432,
  user = "db_user",
  password = "your_password"
)

# Update HPAI data for March 2025
update_woah_db(
  start_date = "2025-03-01",
  end_date = "2025-03-31",
  disease_name = "Influenza A virus",
  db_params = db_params
)
}
}
