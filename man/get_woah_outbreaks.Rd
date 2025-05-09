% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/hello.R
\name{get_woah_outbreaks}
\alias{get_woah_outbreaks}
\title{Fetch Outbreak Event Data from WOAH API}
\usage{
get_woah_outbreaks(
  start_date,
  end_date,
  disease_name = NULL,
  language = "en",
  page_size = 100,
  max_pages = 50,
  verbose = FALSE
)
}
\arguments{
\item{start_date}{Character string or Date object for the start of the event date range (YYYY-MM-DD).}

\item{end_date}{Character string or Date object for the end of the event date range (YYYY-MM-DD).}

\item{disease_name}{Optional: Character string specifying the exact disease name.
If NULL or empty, fetches data for all diseases. Uses internal `get_woah_disease_id`.}

\item{language}{Language code (default: "en").}

\item{page_size}{Number of records to fetch per API call (default: 100).}

\item{max_pages}{Safety limit: Maximum number of pages to fetch (default: 50).}

\item{verbose}{Logical: Print progress messages? (Default: FALSE)}
}
\value{
A data frame containing the combined outbreak data from all pages,
  an empty data frame if no records are found, or NULL if an error occurs.
}
\description{
Retrieves outbreak event data (tabular format) from the WOAH API,
allowing filtering by disease name and date range. Handles pagination automatically.
}
\examples{
\dontrun{
  # Get Foot and Mouth disease outbreaks in March 2025
  fmd_data <- get_woah_outbreaks(
    start_date = "2025-03-01",
    end_date = "2025-03-26",
    disease_name = "Foot and mouth disease virus (Inf. with) "
  )
  if (!is.null(fmd_data)) print(utils::head(fmd_data))

  # Get all outbreaks in the first week of March 2025
  all_data <- get_woah_outbreaks(
    start_date = "2025-03-01",
    end_date = "2025-03-07",
    disease_name = NULL # Or omit the parameter
  )
  if (!is.null(all_data)) print(utils::head(all_data))
}
}
