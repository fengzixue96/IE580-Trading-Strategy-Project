# FUNCTIONS:
#---------------------------------------
#filter DTV
# function to implement exponential moving average DTV:
ema.dtv = function(pv, k){
  # pv: XTS vector of multiplying the daily volume by the price of the stock
  # k: smooting parameter
  len = length(pv)
  dtv = c(coredata(pv[1]), rep(NA, len-1))
  for(i in 2:len){
    if(is.na(pv[i])){
      dtv[i] = dtv[i-1]
    } else {
      dtv[i] = k * pv[i]  + (1-k)*dtv[i-1]	
    }
  }
  return(dtv)
}

#iii)	Volatility-adjusted return -- vol
# function to implement exponential moving average volatility estimate:
ema.vol = function(r, k){
  # r: XTS vector of periodic returns
  # k: smooting parameter
  # funciton assumes r[1] == 0
  len = length(r)
  vol = c(0, rep(NA, len-1))
  for(i in 2:len){
    if(is.na(r[i])){
      vol[i] = vol[i-1]
    } else {
      vol[i] = k * abs(as.numeric(r[i])) + (1-k)*vol[i-1]	
    }
  }
  return(vol)
}

#delta V3m &V1m
# function to calculate percentage change in average trading volume relative 
# to average trading volume over the last year:
#---------------------------------------
PctVolDelta = function(x, period){
  # x: xts object of daily share volume
  # period: rolling time window in trade days
  # Returns: xts object containing percentage change in 1MO or 3MO 
  # 		   average trading volume relative to average 1 year trading volume. 
  
  
  # calculate periodic rolling volume averages:
  vol.avg.1yr = rollapply(x, 252, FUN = mean, fill = NA, align = "right")
  vol.avg.period = rollapply(x, period, FUN = mean, fill = NA, align = "right")
  
  # calculate percentage change:
  vol.pct.delta = (vol.avg.period/vol.avg.1yr) - 1
  
  # out:
  return(vol.pct.delta)
}


# function to calculate identify peaks and calculate resistance levels:
#---------------------------------------
RLxts = function(x, period, nup){
  # x: xts object of price data, trading days only
  # period: rolling time window for peak calculation
  # nup: number of leading rising periods to define a 
  #      peak
  # Return: xts object containing time series of 
  #         resistance levels and percent difference
  #         from nearest resistance level
  #         
  
  # calculate peaks:
  peaks = rollapply(x, 
                    period, 
                    align = "right",
                    FUN = function(x) which.max(x) == nup)
  
  # create resistance level time series:
  rlts = na.locf(merge(x, x[index(peaks[peaks==1])], join = "left"))
  colnames(rlts) = c("PRC", "RL")
  rlts$PctDiff = rlts$PRC/rlts$RL - 1
  
  # out:
  return(rlts)
}


##########################SubPortfolio Construction#############################
subPort = function(x, td, hp, hist, lag, beta, pos.size, NL, NS, ftype = "tech", tune = TRUE, linear = FALSE, rand = FALSE){
  # x: full training data data frame with
  #	 features and targets
  # td: model training date
  # hist: training history in days
  # lag: trade day window to lag training history
  # hp: holding period
  # beta: quantile used to identify tail sets
  # pos.size: position size in ETF shares
  # NL: number of long positons to take
  # NS: number of short positions to take
  # ftype: type of feature sets
  # tune: logical whether to tune the meta parameters of SVMs
  # linear: logical whether to use linear SVM or nonlinear SVM
  # rand:  logical whether to select a random portfolio
  # Return: entry and exit transaction data
  
  
  # identify training range, entry date, exit date:
  #--------------------------------------
  uniq.dates = sort(unique(x$Date))
  train.start = uniq.dates[which(uniq.dates == td) - (hist+lag)]
  train.end = uniq.dates[which(uniq.dates == td) - (lag)]
  test.date = uniq.dates[which(uniq.dates == td)]
  entry.date = uniq.dates[which(uniq.dates == td) + 1]
  exit.date = uniq.dates[which(uniq.dates == td + hp)]
  
  sec.train.start<- unique(x[x$Date == train.start,"PERMNO"])
  sec.train.end<- unique(x[x$Date == train.end,"PERMNO"])
  sec.test<- unique(x[x$Date == test.date,"PERMNO"])
  sec.entry<- unique(x[x$Date == entry.date,"PERMNO"])
  sec.exit<- unique(x[x$Date == exit.date,"PERMNO"])
  
  sec = unique(Reduce(intersect, list(sec.train.start,sec.train.end,sec.test,sec.entry,sec.exit)))
  
  x = x[x$PERMNO %in% sec,]
  
  ########apply date filters here#########
  # LIQ, DTV, PRC quantiles:
  data.filter = x[x$Date == test.date,]
  liq.quant = quantile(data.filter$liq,probs = 0.5)
  dtv.quant = quantile(data.filter$dtv,probs = 0.5)
  prc.quant = quantile(data.filter$PRC,probs = 0.5)
  
  # LIQ, DTV, PRC classes:
  data.filter$liq.class = ifelse(data.filter$liq <= liq.quant, 1, 0)
  data.filter$dtv.class = ifelse(data.filter$dtv >= dtv.quant, 1, 0)
  data.filter$prc.class = ifelse(data.filter$PRC >= prc.quant, 1, 0)
  
  data.filter$filter = data.filter$liq.class * data.filter$dtv.class * data.filter$prc.class
  sec.filter = unique(data.filter[data.filter$filter == 1,"PERMNO"])
  
  x = x[x$PERMNO %in% sec.filter,]
  
  # cache exit prices:
  exit.pxs = x[x$Date == exit.date, c("Date", "PERMNO", "PRC")]
  
  # add two checkpoints for monthly return
  first.month.date = uniq.dates[which(uniq.dates == td + (train.dates[i+1]-train.dates[i]))]
  second.month.date = uniq.dates[which(uniq.dates == td + (train.dates[i+2]-train.dates[i]))]
  first.month.pxs = x[x$Date == first.month.date, c("Date", "PERMNO", "PRC")]
  second.month.pxs = x[x$Date == second.month.date, c("Date", "PERMNO", "PRC")]
  
  # training label
  label = "rtn.class"
  
  # training features
  tech.features = c(
    # 7 technical features
    "mom.3MO",
    "mom.1YR",
    "vol.delta.3MO",
    "vol.delta.1MO",
    "n.high.low",
    "max.rtn",
    "rl.pct.diff"
  )
  
  fund.features = c(
    # 44 fundamental features
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
    "chechy",
    "sac",
    "abbs",
    "abcf",
    "fh",
    "wa",
    "qr",
    "dpr",
    "bv",
    "bvtd",
    "rs",
    "da",
    "de",
    "ca",
    "li",
    "re",
    "ss"
  )
  
  if(ftype == "tech") {
    svm.cols = c(tech.features,label)
  } else if (ftype == "fund") {
    svm.cols = c(fund.features, label)
  } else {
    svm.cols = c(tech.features,fund.features,label)
  }
  
  # cache testing data and entry prices:
  test.tkrs = x[x$Date == test.date, "PERMNO"] 
  test.dat = x[x$Date == test.date, colnames(x) %in% svm.cols]
  entry.dat = x[x$Date == entry.date, ]
  
  if(rand == FALSE){
    
    # subset data to trainig history:
    x = x[x$Date >= train.start & x$Date <= train.end, ]
    
    
    # identify tail sets and return classes:
    #--------------------------------------
    # return quantiles:
    rtn.quants = quantile(x[, "vol.adj.rtn"], 
                          probs = c(beta, 1-beta))
    # return classes:
    x$rtn.class = ifelse(x$vol.adj.rtn > rtn.quants[2], "L", 
                         ifelse(x$vol.adj.rtn < rtn.quants[1], "S", 0))
    
    # subset to tail sets and drop target return:
    x = x[!(x$rtn.class == 0), ]
    rownames(x) = NULL
    x$rtn.class = as.factor(x$rtn.class)
    
    # correlation plot of features
    #x_corr = x[, colnames(x) %in% svm.cols]
    #x_corr$rtn.class = ifelse(x_corr$rtn.class == "L", 1, 
    #                     ifelse(x_corr$rtn.class =="S", -1, 0))
    #corr = (cor(x_corr))
    #library(corrplot)
    #corrplot(corr)
    
    # tune, fit, and forecast svm in parallel (caret):
    #--------------------------------------
    if (tune) {
      tunegrid = expand.grid(sigma = c(0.5,1,2,4), C = c(0.5,1,2,4))
      trcontrol = trainControl(
        method = "cv",
        number = 5,
        savePred = TRUE,
        classProb = TRUE
      )
    } else {
      tunegrid = expand.grid(sigma = 0.5, C = 2)
      trcontrol = trainControl(
        method = "none",
        savePred = TRUE,
        classProb = TRUE
      )
    }
    
    if(linear) {
      svm.type = "svmLinear"
      tunegrid = expand.grid(C = c(0.5,1,2,4))
    } else {
      svm.type = "svmRadial"
    }
    
    #set.seed(1701)
    attempt = 1
    while (attempt <= 10) {
      print(attempt)
      attempt = attempt + 1
      tryCatch({
        svmfit = caret::train(
          rtn.class ~ .,
          data = na.omit(x[, colnames(x) %in% svm.cols]),
          method = svm.type,
          preProcess = c("center", "scale"),
          trControl = trcontrol,
          tuneGrid = tunegrid
        )
        # forecast class and probabilities:
        rm(forecastClass)
        rm(forecastProbs)
        forecastClass = predict(object = svmfit,
                                newdata = test.dat)
        forecastProbs = predict(object = svmfit,
                                newdata = test.dat,
                                type = "prob")
      }
      , error = function(e) {
        print("catched")
        print(e)
      })
      if (exists("forecastProbs") == TRUE) attempt = 11
    }
    
    
    # identify entry trades:
    #--------------------------------------
    # extract descions values and append ticker and closing price:
    Class = forecastClass
    Prob = forecastProbs
    TKR = as.vector(test.tkrs)
    entry.trans = data.frame("predClass" = Class,
                             "ProbLong" = Prob[,colnames(Prob) == 'L'],
                             "ProbShort" = Prob[,colnames(Prob) == 'S'],
                             "TKR" = TKR,
                             "TxnPrice" = entry.dat$PRC,
                             "TxnDate" = entry.date,
                             "Transaction" = "Enter",
                             stringsAsFactors = FALSE)
    entry.trans = entry.trans[order(entry.trans$ProbLong), ]
    
  } else {
    entry.trans = entry.dat[, c("PERMNO","PRC", "Date")]
    colnames(entry.trans) = c("TKR", "TxnPrice", "TxnDate")
    entry.trans$Transaction = "Enter"
  }
  
  # final subportfolio
  #--------------------------------------
  if(rand){
    sample = entry.trans[sample(nrow(entry.trans), NL + NS), ]
    longs = sample[1:NL, ]
    shorts = sample[-(1:NL), ]
    
    longs$Direction = 'long'
    shorts$Direction = 'short'
    
    longs$TxnQty = (as.numeric(pos.size)/NL)/longs$TxnPrice
    shorts$TxnQty = (-as.numeric(pos.size)/NS)/shorts$TxnPrice
    
  } else {
    dirs = unique(entry.trans$predClass)
    if("L" %in% dirs) {
      longs = entry.trans[entry.trans$predClass == "L", ]
      nlong = dim(longs)[1]
      longs = longs[order(longs$ProbLong, decreasing = T), ]
      longs = longs[1:min(nlong, NL), ]
      longs$Direction = 'long'
      longs$TxnQty = (as.numeric(pos.size)/NL)/longs$TxnPrice
    } else {
      longs = NULL
    }
    if("S" %in% dirs) {
      shorts = entry.trans[entry.trans$predClass == "S", ]
      nshort = dim(shorts)[1]
      shorts = shorts[order(shorts$ProbShort, decreasing = T), ]
      shorts = shorts[1:min(nshort, NS), ]
      shorts$Direction = 'short'
      shorts$TxnQty = (-as.numeric(pos.size)/NS)/shorts$TxnPrice
    } else {
      shorts = NULL
    }
  }
  
  entry.trans = rbind(longs, shorts)
  entry.trans$MktValue = entry.trans$TxnPrice * entry.trans$TxnQty
  entry.trans$MktValue.1m = entry.trans$MktValue
  entry.trans$MktValue.2m = entry.trans$MktValue
  
  # append exit trades:
  exit.trans = entry.trans
  exit.trans$TxnDate = exit.date
  exit.trans$Transaction = "Exit"
  exit.trans$TxnQty = -entry.trans$TxnQty
  
  # match exit price:
  match.px = match(entry.trans$TKR, exit.pxs[exit.pxs$PERMNO %in% entry.trans$TKR, 'PERMNO'])
  exit.trans$TxnPrice = exit.pxs[exit.pxs$PERMNO %in% entry.trans$TKR, "PRC"][match.px]
  exit.trans$MktValue = exit.trans$TxnPrice * exit.trans$TxnQty
  
  #match 1M price:
  match.px.1m = match(entry.trans$TKR, first.month.pxs[first.month.pxs$PERMNO %in% entry.trans$TKR, 'PERMNO'])
  exit.trans$TxnPrice.1m = first.month.pxs[first.month.pxs$PERMNO %in% entry.trans$TKR, "PRC"][match.px.1m]
  exit.trans$MktValue.1m = exit.trans$TxnPrice.1m * exit.trans$TxnQty
  
  #match 2M price:
  match.px.2m = match(entry.trans$TKR, second.month.pxs[second.month.pxs$PERMNO %in% entry.trans$TKR, 'PERMNO'])
  exit.trans$TxnPrice.2m = second.month.pxs[second.month.pxs$PERMNO %in% entry.trans$TKR, "PRC"][match.px.2m]
  exit.trans$MktValue.2m = exit.trans$TxnPrice.2m * exit.trans$TxnQty
  
  drops <- c("TxnPrice.1m","TxnPrice.2m")
  exit.trans = exit.trans[ , !(names(exit.trans) %in% drops)]
  
  # all transactions:
  #--------------------------------------
  trans = rbind(entry.trans, exit.trans)
  
  # append actual realized performance and classes:
  PL = tapply(trans$MktValue, as.vector(trans$TKR), function(x) -sum(x))
  pxDiff = tapply(trans$TxnPrice, as.vector(trans$TKR), diff)
  PL.1m = tapply(trans$MktValue.1m, as.vector(trans$TKR), function(x) -sum(x))
  PL.2m = tapply(trans$MktValue.2m, as.vector(trans$TKR), function(x) -sum(x))
  PL["Total"] = sum(PL)
  PL["eomEquity"] = pos.size + PL["Total"]
  PL["Total.1m"] =  sum(PL.1m)
  PL["Total.2m"] =  sum(PL.2m)
  PL["eomEquity.1m"] = pos.size + PL["Total.1m"]
  PL["eomEquity.2m"] = pos.size + PL["Total.2m"]
  PL["logRtn"] = log(PL["eomEquity"]) - log(pos.size)
  
  if(rand == FALSE){
    
    # realized classes:
    classes = trans[trans$Transaction == "Enter", c("TKR", "TxnDate", "predClass")]
    classes$predClass = as.character(classes$predClass)
    classes$actClasses = ifelse(pxDiff[match(classes$TKR, names(pxDiff))] >= 0, "L", "S")
    classes$pl = PL[match(classes$TKR, names(PL))]
    
    out = list("Transactions" = trans, 
               "PandL" = PL, 
               "Classes" = classes,
               "svmOptpars" = svmfit$bestTune,
               "svmAcc" = max(svmfit$results$Accuracy),
               "svmImp" = varImp(svmfit))
    # rm(svmfit)
  } else {
    
    out = list("Transactions" = trans, 
               "PandL" = PL)		
  }
  return(out)
}