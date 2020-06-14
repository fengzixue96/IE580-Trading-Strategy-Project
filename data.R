setwd("F:/data")

library(xts)

## data.R                                                                     ##
## ------------------                                                         ##
## Functions for loading, saving, subsetting, merging and cleaning data       ##
##                                                                            ##
setwd("F:/data/Compustat_1961-2019-06")
load_raw_data <- function(file_name, seperator) {
  # ==========================================================================
  # Load the raw CRSP/Compustat data (tab delimited)
  #
  # Args:
  # ----------
  # file_name file_name
  # seperator field seperator (e.g. tab delimited)
  #
  # Return:
  # ---------
  # The raw data
  # ==========================================================================
  if (seperator == "t") {
    return(read.delim(
      file = file_name,
      header = TRUE,
      as.is = TRUE,
      quote = ""
    ))
  }
}

seperate_securities_gics <- function(data, gics, id) {
  # ==========================================================================
  # Seperate the data into GICS Sectors
  #
  # Args:
  # ----------
  # data  the data
  # gics  Global Industry Classification Standard
  # id  the security id
  #
  # Return:
  # ---------
  # A matrix of the ids in each specified GICS classification
  # ==========================================================================
  if (gics == "GSECTOR") {
    # US company only
    data = data[data$FIC == 'USA', ]
    
    # Sectors defined by the Global Industry Classification Standard (GICS)
    gsector_names <- c(
      "Energy",
      "Materials",
      "Industrials",
      "Consumer_Discretionary",
      "Consumer_Staples",
      "Health_Care",
      "Financials",
      "Information_Technology",
      "Telecommunication_Services",
      "Utilities"
##      "Unclassified"
    )
    gsector_codes <- c("10", "15", "20", "25", "30", "35",
                       "40", "45", "50", "55","")
    sectors <- matrix(data = list(),
                      nrow = length(gsector_codes),
                      ncol = 1)
    rownames(sectors) <- gsector_names
    # Now search for each GSECTOR code
    for (i in 1:length(gsector_codes)) {
      sectors[i, ] <-
        list(sort(unique(na.omit(data[data$GSECTOR == gsector_codes[i], id]))))
    }
    return(sectors)
  }
}

gvkey2Permno <- NULL
crsp_daily_1981_2018 <- NULL
computstatQuery <- NULL

gvkey2Permno_subset <- NULL
crsp_daily_1981_2018_subset <- NULL
computstatQuery_subset <- NULL

sp500_1981_2018 <- NULL

crsp_daily_1981_2018_xts <- NULL
sp500_1981_2018_xts <- NULL

crsp_daily_xts <- NULL
crsp_daily_Energy_xts <- NULL
crsp_daily_Materials_xts <- NULL
crsp_daily_Industrials_xts <- NULL
crsp_daily_Consumer_Discretionary_xts <- NULL
crsp_daily_Consumer_Staples_xts <- NULL
crsp_daily_Health_Care_xts <- NULL
crsp_daily_Financials_xts <- NULL
crsp_daily_Information_Technology_xts <- NULL
crsp_daily_Telecommunication_Services_xts <- NULL
crsp_daily_Utilities_xts <- NULL
crsp_securities_sectors <- NULL

gvkey2Permno_fields <-
  c("GSECTOR",
    "gvkey",
    "LPERMNO",
    "FIC")

crsp_daily_fields <- c("date", "PERMNO", "PRC", "RETX", "VOL")

computstat_fields <-
  c(
    "GVKEY",
    "fyearq",
    "datacqtr",
    "fdateq",
    "rdq",
    "fic",
    "gsector",
    #28 fundamental features
    "actq",
    "atq",
    "ceqq",
    "chq",
    "cshoq",
    "dlcq",
    "dlttq",
    "dpq",
    "dvpq",
    "epsfxq",
    "invtq",
    "ivstq",
    "lctq",
    "ltq",
    "niq",
    "oancfy",
    "piq",
    "rectq",
    "revtq",
    "txtq",
    "uniamiq",
    "uopiq",
    "wcapq",
    "xoprq",
    "glaq",
    "rectq",
    "lltq",
    "chechy"
  )


data_processing <- function() {
  # ==========================================================================
  # Load, create xts, merge, subset, list securities, seperate, link & align
  #
  # CompustatQuery: compustat table
  # Gvkey2Permno: link table
  # CRSP_DAILY_1981_2018: CRSP daily data from 1981-2018
  # ==========================================================================
  # --------------------------------------------------------------------------
  # Load and Subset the data
  # --------------------------------------------------------------------------
  gvkey2Permno <- load_raw_data(file = "data/gvkey2Permno.txt",
                                seperator = "t")
  dim(gvkey2Permno)
  gvkey2Permno_subset <-
    gvkey2Permno[, gvkey2Permno_fields]
  dim(gvkey2Permno_subset)
  gvkey2Permno <- NULL
  gc()
  
  crsp_daily_1981_2018 <-
    load_raw_data(file = "data/CRSP_DAILY_1981_2018.txt", seperator = "t")
  dim(crsp_daily_1981_2018)
  crsp_daily_1981_2018_subset <-
    crsp_daily_1981_2018[, crsp_daily_fields]
  dim(crsp_daily_1981_2018_subset)
  
  sp500_1981_2018 <- crsp_daily_1981_2018[, c("date", "sprtrn")]
  
  crsp_daily_1981_2018 <- NULL
  gc()
  
  computstatQuery <-
    load_raw_data(file = "data/compustat.txt",
                  seperator = "t")
  dim(computstatQuery)
  computstatQuery_subset <- computstatQuery[, colnames(computstatQuery) %in% computstat_fields]
  dim(computstatQuery_subset)
  computstatQuery <- NULL
  gc()
  
  # --------------------------------------------------------------------------
  # Save the data as .RDS
  # --------------------------------------------------------------------------
  saveRDS(gvkey2Permno_subset, file = "data/gvkey2Permno_subset.rds")
  saveRDS(crsp_daily_1981_2018_subset, file = "data/crsp_daily_1981_2018_subset.rds")
  saveRDS(computstatQuery_subset, file = "data/computstatQuery_subset.rds")
  saveRDS(sp500_1981_2018, file = "data/sp500_1981_2018.rds")
  gc()
  
  # --------------------------------------------------------------------------
  # Create an xts time series of the timeseries data
  # --------------------------------------------------------------------------
  crsp_daily_1981_2018_xts <-
    xts(
      x = crsp_daily_1981_2018_subset[, -1],
      order.by = as.Date(crsp_daily_1981_2018_subset[, 1],
                         format = "%Y/%m/%d")
    )
  dim(crsp_daily_1981_2018_xts)
  crsp_daily_1981_2018_subset <- NULL
  
  sp500_1981_2018_xts <- xts(x = sp500_1981_2018[, -1],
                             order.by = as.Date(sp500_1981_2018[, 1],
                                                format = "%Y/%m/%d"))
  
  sp500_1981_2018_xts <-
    make.index.unique(sp500_1981_2018_xts, drop = TRUE)
  sp500_1981_2018 <- NULL
  gc()
  
  saveRDS(crsp_daily_1981_2018_xts, file = "data/crsp_daily_1981_2018_xts.rds")
  saveRDS(sp500_1981_2018_xts, file = "data/sp500_1981_2018_xts.rds")
  gc()
  
  # --------------------------------------------------------------------------
  # Seperate securities into sectors
  # --------------------------------------------------------------------------
  ##gvkey2Permno_subset <- load_saved_data(file_name = "data/gvkey2Permno_subset",
  ##                                       file_ext = ".rds")
  crsp_securities_sectors <- 
    seperate_securities_gics(data = gvkey2Permno_subset,
                             gics = "GSECTOR",
                             id = "LPERMNO")
  # Counts of each
  crsp_securities_sectors
  
  saveRDS(crsp_securities_sectors, file = "data/crsp_securities_sectors.rds")
  
  # --------------------------------------------------------------------------
  # Find all CRSP securities within each sector (PERMNO).
  # Let's get the exact range (1981 to end-2018).
  # --------------------------------------------------------------------------
  crsp_daily_xts = crsp_daily_1981_2018_xts
  crsp_daily_1981_2018_xts <- NULL
  gc()
  
  crsp_daily_xts = readRDS(file = "data/crsp_daily_1981_2018_xts.rds")
  crsp_securities_sectors = readRDS(file = "data/crsp_securities_sectors.rds")
  
  crsp_daily_Energy_xts <-
    crsp_daily_xts[crsp_daily_xts$PERMNO %in%
                     c((crsp_securities_sectors["Energy", ])[[1]]), ]
  crsp_daily_Materials_xts <-
    crsp_daily_xts[crsp_daily_xts$PERMNO %in%
                     c((crsp_securities_sectors["Materials", ])[[1]]), ]["1981-01-01::2018-12-31"]
  crsp_daily_Industrials_xts <-
    crsp_daily_xts[crsp_daily_xts$PERMNO %in%
                     c((crsp_securities_sectors["Industrials", ])[[1]]), ]
  crsp_daily_Consumer_Discretionary_xts <-
    crsp_daily_xts[crsp_daily_xts$PERMNO %in%
                     c((crsp_securities_sectors["Consumer_Discretionary", ])[[1]]), ]
  crsp_daily_Consumer_Staples_xts <-
    crsp_daily_xts[crsp_daily_xts$PERMNO %in%
                     c((crsp_securities_sectors["Consumer_Staples", ])[[1]]), ]
  crsp_daily_Health_Care_xts <-
    crsp_daily_xts[crsp_daily_xts$PERMNO %in%
                     c((crsp_securities_sectors["Health_Care", ])[[1]]), ]
  crsp_daily_Financials_xts <-
    crsp_daily_xts[crsp_daily_xts$PERMNO %in%
                     c((crsp_securities_sectors["Financials", ])[[1]]), ]
  crsp_daily_Information_Technology_xts <-
    crsp_daily_xts[crsp_daily_xts$PERMNO %in%
                     c((crsp_securities_sectors["Information_Technology", ])[[1]]), ]
  crsp_daily_Telecommunication_Services_xts <-
    crsp_daily_xts[crsp_daily_xts$PERMNO %in%
                     c((crsp_securities_sectors["Telecommunication_Services", ])[[1]]), ]
  crsp_daily_Utilities_xts <-
    crsp_daily_xts[crsp_daily_xts$PERMNO %in%
                     c((crsp_securities_sectors["Utilities", ])[[1]]), ]
  
  saveRDS(crsp_daily_Energy_xts, file = "data/crsp_daily_Energy_xts.rds")
  saveRDS(crsp_daily_Materials_xts, file = "data/crsp_daily_Materials_xts.rds")
  saveRDS(crsp_daily_Industrials_xts, file = "data/crsp_daily_Industrials_xts.rds")
  saveRDS(crsp_daily_Consumer_Discretionary_xts, file = "data/crsp_daily_Consumer_Discretionary_xts.rds")
  saveRDS(crsp_daily_Consumer_Staples_xts, file = "data/crsp_daily_Consumer_Staples_xts.rds")
  saveRDS(crsp_daily_Health_Care_xts, file = "data/crsp_daily_Health_Care_xts.rds")
  saveRDS(crsp_daily_Financials_xts, file = "data/crsp_daily_Financials_xts.rds")
  saveRDS(crsp_daily_Information_Technology_xts, file = "data/crsp_daily_Information_Technology_xts.rds")
  saveRDS(crsp_daily_Telecommunication_Services_xts, file = "data/crsp_daily_Telecommunication_Services")
  saveRDS(crsp_daily_Utilities_xts, file = "data/crsp_daily_Utilities_xts.rds")
  
  head(crsp_daily_Financials_xts)
  tail(crsp_daily_Financials_xts)
  dim(crsp_daily_Energy_xts)
  
  crsp_securities_sectors <- NULL
  crsp_daily_Energy_xts <- NULL
  crsp_daily_Materials_xts <- NULL
  crsp_daily_Industrials_xts <- NULL
  crsp_daily_Consumer_Discretionary_xts <- NULL
  crsp_daily_Consumer_Staples_xts <- NULL
  crsp_daily_Health_Care_xts <- NULL
  crsp_daily_Financials_xts <- NULL
  crsp_daily_Information_Technology_xts <- NULL
  crsp_daily_Telecommunication_Services_xts <- NULL
  crsp_daily_Utilities_xts <- NULL
  gc()
}
