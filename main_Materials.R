rm(list=ls()) #清除全部对象
Sys.setenv(TZ = 'UTC') #设置系统市区到“GMT(UTC)”,使其变为默认时区。from 'xts'
setwd("C:/Users/HP/Desktop/Lesson/580 Trading Stretegy/previous/code") #设置工作路径,也就是临时设置工作目录
library(parallel)
library(xts)
library(quantmod)
library(PerformanceAnalytics)
library(caret)

detectCores() #检查当前电脑可用核数

#使用全部核数
if (Sys.info()['sysname'] == "Windows")
{
  library(doParallel) #支持Windows和unix-like
  registerDoParallel(cores = detectCores())
  print("Windows parallel backend 'doParallel' registered
        - required for 'foreach'")
} else {
  library(doMC) #只支持unix-like
  registerDoMC(cores = detectCores())
  print("Unix parallel backend 'doMC' registered - required for 'foreach'")
}

source(file = "UtilityFunctions.R") #“预装“已写好的函数

#自己读取rds
library(data.table)
data <- readRDS("data/svmOptlist1.tech.oos.rds")


start_time <- Sys.time()

# Load our GICS sector seperated CRSP data
crsp_daily_xts <- readRDS(file = "data/crsp_daily_Materials_xts.rds")

# Load the market returns (S&P500)
sp500_xts <- readRDS(file = "data/sp500_1981_2018_xts.rds")

# Load computstat
computstat <- readRDS(file = "data/computstatQuery_subset.rds")

# Load gvkey2Permno
gvkey2Permno <- readRDS(file = "data/gvkey2Permno_subset.rds")

start_date = "2007-01-01"
end_date = "2010-12-31"

# Subset 
sp500_xts <- sp500_xts[paste0(start_date,"::",end_date)]
crsp_daily_xts <-
  crsp_daily_xts[paste0(start_date,"::",end_date)]

#survivial bias
securities_start <- unique(na.omit(crsp_daily_xts[index(sp500_xts)[1], c("PERMNO", "PRC", "RETX")])$PERMNO)
securities_end <- unique(na.omit(crsp_daily_xts[index(sp500_xts)[length(sp500_xts)], c("PERMNO", "PRC", "RETX")])$PERMNO)
securities <- intersect(securities_start,securities_end)


############################Training Data Construction##############################
# parameteres:
target.win = 1

# fundamental indicators
fundamentals <-
  c(
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
sig = function(x) ifelse(is.na(x),0,ifelse(x>0,1,0))
# append data with training labels and features:
for(i in 1:length(securities)){
  print(i)
  x = crsp_daily_xts[crsp_daily_xts$PERMNO == securities[i], c("PERMNO", "PRC", "RETX", "VOL")]
  if(length(x$PERMNO) != length(sp500_xts)) next
  
  storage.mode(x) <- "double"
  
  #clean data
  x$PRC = na.locf(abs(x$PRC),fromLast = TRUE)
  x[x$VOL == 0, "VOL"] <- NA
  x$VOL = na.locf(abs(x$VOL),fromLast = TRUE)
  
  # target return
  # EWMA vol adjusted forward looking log return:
  x$rtn = diff(log(x$PRC), target.win)
  x$fwd.raw.rtn = lag(x$rtn, -target.win)
  x$ewma.vol = ema.vol(x$fwd.raw.rtn, 2/(124+1)) #180
  x$vol.adj.rtn = x$fwd.raw.rtn / x$ewma.vol
  
  # 3MO and 1YR momentum indicators:
  # 3MO and 1Y trailing returns for symbol and sp500:
  x$trailing.3MO.rtn = diff(log(x$PRC), 63)
  x$trailing.1YR.rtn = diff(log(x$PRC), 252)
  x$trailing.3MO.rtn.mkt = rollapply(sp500_xts, 63, FUN = sum, align = "right")
  x$trailing.1YR.rtn.mkt = rollapply(sp500_xts, 252, FUN = sum, align = "right")
  # momentum - 3MO and 1YR excess returns:
  x$mom.3MO = x$trailing.3MO.rtn - x$trailing.3MO.rtn.mkt
  x$mom.1YR = x$trailing.1YR.rtn - x$trailing.1YR.rtn.mkt
  
  # 3MO and 1MO delta volume indicators:
  x$vol.delta.3MO = PctVolDelta(x$VOL, 63)
  x$vol.delta.1MO = PctVolDelta(x$VOL, 21)
  
  # number of 12-month highs and 12-month lows indicator:
  x$n.high = lag(runMax(x$PRC, 252),1)
  x$n.low = lag(runMin(x$PRC, 252),1)
  x$n.high.low = ifelse(x$PRC > x$n.high, 1, 
                        ifelse(x$PRC < x$n.low, -1, 0))
  
  # max daily return indicator:
  x$max.rtn = rollapply(x$rtn, 21, FUN = max, align = "right")
  
  # resistance indicator:
  x$rl.pct.diff = RLxts(na.trim(x$PRC), 21, 21)$PctDiff
  
  # data filters:
  x$pv = x$PRC*x$VOL
  # LIQ
  x$pred = sign(x$rtn) * log(x$pv)
  x$liq = rollapply(x, 63, function(d) coef(lm(rtn~pred, data=d))[2], align = "right", by.column=FALSE)
  
  # DTV
  x$dtv = ema.dtv(x$pv,2/(63+1)) #91
  
  # fundamental features:
  permno = unique(x[,"PERMNO"])
  gvkey = gvkey2Permno[gvkey2Permno$LPERMNO == permno,"gvkey"]
  comp = computstat[(computstat$GVKEY == gvkey),]
  comp = comp[order(comp$fdateq), ]
  dates = index(x)
  fund = data.frame(matrix(ncol = 28, nrow = length(dates)))
  colnames(fund) <- fundamentals
  for(j in 1:length(dates)){
    # use fdateq first
    comp$diff = as.numeric(as.Date(as.character(comp$fdateq), format("%Y/%m/%d"))
                           -as.Date(as.character(dates[j])))
    match = tail(comp[comp$diff<=0,fundamentals],1)
    if (nrow(match)==1) {
      fund[j,] = t(match)
      next
    }
    # use rdq as fall back
    comp$diff = as.numeric(as.Date(as.character(comp$rdq), format("%Y/%m/%d"))
                           -as.Date(as.character(dates[j]))) + 31 #45
    match = tail(comp[comp$diff<=0,fundamentals],1)
    if (nrow(match)==1) {
      fund[j,] = t(match)
      next
    }
  }
  
  fund = cbind(dates, fund)
  fund$sac = fund$actq - fund$chq - fund$lctq + fund$dlcq
  fund$abbs = fund$sac - lag(fund$sac, 252)
  fund$abcf = fund$uniamiq-fund$oancfy
  fund$fh = sig(fund$uniamiq)+sig(fund$oancfy)+sig(diff(fund$uniamiq, 252))+sig(-fund$abcf)+
    sig(-diff(fund$dlttq/fund$atq, 252))+sig(diff(fund$actq/fund$lctq, 252))
  fund$wa = fund$actq-fund$lctq
  fund$qr = (fund$actq-fund$invtq)/fund$lctq
  fund$dpr = fund$dvpq/fund$niq
  fund$bv = fund$atq-fund$ltq
  fund$bvtd = fund$bv-fund$dlcq
  fund$rs = fund$rectq/fund$revtq
  fund$da = fund$dlcq/fund$atq
  fund$de = fund$dlcq/fund$ceqq
  fund$ca = fund$chq/fund$atq
  fund$li = fund$ltq/fund$niq
  fund$re = fund$niq/fund$ceqq
  fund$ss = fund$revtq/fund$cshoq
  fund = xts(fund[,-1], order.by=fund[,1])
  x = merge(x,fund)
  
  x = na.locf(x, fromLast = TRUE)
  x[is.na(x)] = -1
  
  if(i == 1){
    train.data = x
  } else {
    train.data = rbind(train.data, x)
  }
  rm(x)
}

#saveRDS(train.data, "data/train.data.tech.fund.rds")

#train.data = readRDS("data/train.data.tech.fund.oos.rds")

# clean training data:
train.data = train.data[complete.cases(train.data), ]
train.data = train.data[order(index(train.data)), ]
train.data = as.data.frame(train.data)
tdf = cbind(Date = as.Date(rownames(train.data)), train.data)

#saveRDS(tdf, "data/tdf.tech.oos.2.rds")

#tdf = readRDS("data/tdf.rds")


#################################Portfolio Construction#################################
# training dates:
uniq.dates = sort(unique(tdf$Date))

# first and last trading date:
start = as.Date("2008-04-01")
end = as.Date("2010-10-01")

month.end  = seq(start, end,  by = "month") - 1

idx = rep(NA, length(month.end))

# isloate closest trading day to month end:
for(i in 1:length(month.end)){
  date.diff = month.end[i] - uniq.dates
  if(month.end[i] %in% uniq.dates){
    idx[i] = match(month.end[i], uniq.dates)
  } else {
    idx[i] = which.min(pmax(date.diff, 0)) - 1
  }
}

train.dates = uniq.dates[idx]

# account equity, transaction, and model tuning containers:
#---------------------------------------
# model list containers:
svmOptlist1 = list(NA)
randlist1 = list(NA)

svmOptlist2 = list(NA)
randlist2 = list(NA)

svmOptlist3 = list(NA)
randlist3 = list(NA)

# equity:
equity = xts(matrix(1e6, ncol = 6, nrow = length(train.dates)), 
             order.by = train.dates)
colnames(equity) = c("svmOpt1", "svmOpt2", "svmOpt3", "rand1", "rand2", "rand3")

#saveRDS(equity, "data/equity.tech.rds")

registerDoMC(detectCores())
################## main loop to generate transactions:##################
#-----------------------------------------------------------------------
#parameters
hist = 60
lag = 1
beta = 0.45
NL  = 10
NS =  10
ftype = "tech"
tune = TRUE
linear = FALSE

a = 1:(length(train.dates)-5)
b = a[seq(1, length(a), 3)]
# The first portfolio
print("Processing the first portfolio...")
for(i in b){
  # optimal svm model:
  print(paste(i, "Processing...", sep = " "))
  
  svmOptlist1[[i]] = subPort(tdf,
                             train.dates[i],
                             train.dates[i+3]-train.dates[i],
                             hist,
                             lag,
                             beta,
                             equity[i,"svmOpt1"],
                             NL,
                             NS,
                             ftype,
                             tune,
                             linear,
                             rand = FALSE)
  equity[i+1,"svmOpt1"] = svmOptlist1[[i]]$PandL["eomEquity.1m"]
  equity[i+2,"svmOpt1"] = svmOptlist1[[i]]$PandL["eomEquity.2m"]
  equity[i+3,"svmOpt1"] = svmOptlist1[[i]]$PandL["eomEquity"]
  print(svmOptlist1[[i]]$PandL["Total"])
  
  # random long short test:
  randlist1[[i]] = subPort(tdf, 
                           train.dates[i],
                           train.dates[i+3]-train.dates[i],
                           hist, 
                           lag,
                           beta,
                           equity[i,"rand1"],
                           NL,
                           NS,
                           ftype,
                           tune,
                           linear,
                           rand = TRUE)
  equity[i+1,"rand1"] = randlist1[[i]]$PandL["eomEquity.1m"]
  equity[i+2,"rand1"] = randlist1[[i]]$PandL["eomEquity.2m"]
  equity[i+3,"rand1"] = randlist1[[i]]$PandL["eomEquity"]
  print(randlist1[[i]]$PandL["Total"])
}

#The second portfolio
print("Processing the second portfolio...")
for(i in (b+1)){
  # optimal svm model:
  print(paste(i, "Processing...", sep = " "))
  
  svmOptlist2[[i]] = subPort(tdf,
                             train.dates[i],
                             train.dates[i+3]-train.dates[i],
                             hist,
                             lag,
                             beta,
                             equity[i,"svmOpt2"],
                             NL,
                             NS,
                             ftype,
                             tune,
                             linear,
                             rand = FALSE)
  equity[i+1,"svmOpt2"] = svmOptlist2[[i]]$PandL["eomEquity.1m"]
  equity[i+2,"svmOpt2"] = svmOptlist2[[i]]$PandL["eomEquity.2m"]
  equity[i+3,"svmOpt2"] = svmOptlist2[[i]]$PandL["eomEquity"]
  print(svmOptlist2[[i]]$PandL["Total"])
  
  # random long short test:
  randlist2[[i]] = subPort(tdf, 
                           train.dates[i],
                           train.dates[i+3]-train.dates[i],
                           hist, 
                           lag,
                           beta,
                           equity[i,"rand2"],
                           NL,
                           NS,
                           ftype,
                           tune,
                           linear,
                           rand = TRUE)
  equity[i+1,"rand2"] = randlist2[[i]]$PandL["eomEquity.1m"]
  equity[i+2,"rand2"] = randlist2[[i]]$PandL["eomEquity.2m"]
  equity[i+3,"rand2"] = randlist2[[i]]$PandL["eomEquity"]
  print(randlist2[[i]]$PandL["Total"])
}

#The third portfolio
print("Processing the third portfolio...")
for(i in (b+2)){
  # optimal svm model:
  print(paste(i, "Processing...", sep = " "))
  
  svmOptlist3[[i]] = subPort(tdf,
                             train.dates[i],
                             train.dates[i+3]-train.dates[i],
                             hist,
                             lag,
                             beta,
                             equity[i,"svmOpt3"],
                             NL,
                             NS,
                             ftype,
                             tune,
                             linear,
                             rand = FALSE)
  equity[i+1,"svmOpt3"] = svmOptlist3[[i]]$PandL["eomEquity.1m"]
  equity[i+2,"svmOpt3"] = svmOptlist3[[i]]$PandL["eomEquity.2m"]
  equity[i+3,"svmOpt3"] = svmOptlist3[[i]]$PandL["eomEquity"]
  print(svmOptlist3[[i]]$PandL["Total"])
  
  # random long short test:
  randlist3[[i]] = subPort(tdf, 
                           train.dates[i],
                           train.dates[i+3]-train.dates[i],
                           hist, 
                           lag,
                           beta,
                           equity[i,"rand3"],
                           NL,
                           NS,
                           ftype,
                           tune,
                           linear,
                           rand = TRUE)
  equity[i+1,"rand3"] = randlist3[[i]]$PandL["eomEquity.1m"]
  equity[i+2,"rand3"] = randlist3[[i]]$PandL["eomEquity.2m"]
  equity[i+3,"rand3"] = randlist3[[i]]$PandL["eomEquity"]
  print(randlist3[[i]]$PandL["Total"])
}
registerDoSEQ()	

#saveRDS(svmOptlist1, "data/svmOptlist1.tech.25.oos.rds")
#saveRDS(svmOptlist2, "data/svmOptlist2.tech.25.oos.rds")
#saveRDS(svmOptlist3, "data/svmOptlist3.tech.25.oos.rds")

#saveRDS(equity, "data/equity.tech.25.oos.rds")


#########################Performance Evaluation##############################
# assign realized classes and construct confusion matricies:
# svm model classes:
svmClasses = lapply(svmOptlist1, function(x) x$Classes)
svmClasses = do.call("rbind", svmClasses)
svm.cm = confusionMatrix(as.factor(svmClasses$predClass), 
                         as.factor(svmClasses$actClass))
svmClasses$accuracy = ifelse(svmClasses$predClass == svmClasses$actClasses, 
                             "Right", 
                             "Wrong")

#equity = readRDS(file = "data/equity.tech.50.rds")

# performance reporting:
equity$svmOpt = equity$svmOpt1+equity$svmOpt2+equity$svmOpt3
equity$rand = equity$rand1+equity$rand2+equity$rand3

drops <- c("svmOpt1","svmOpt2","svmOpt3","rand1","rand2","rand3")
equity.total = equity[,!(names(equity) %in% drops)]

returns = diff(log(equity.total[4:(length(train.dates)-3)]))
mkt.rtns = rollapply(sp500_xts, 21, FUN = sum, align = "right")
mkt.rtns = mkt.rtns[index(mkt.rtns) %in% train.dates[4:(length(train.dates)-3)]]
returns = merge(returns, mkt.rtns)
returns$svmOpt[1]=1
returns$mkt.rtns[1]=1
returns$rand[1]=1

#saveRDS(returns, "data/returns.tech.oos.rds")

#returns = readRDS("data/returns.tech.oos.rds")

charts.PerformanceSummary(returns,
                          colorset = set8equal,
                          main = "SVM Model Performance"
)

Return.annualized(returns)
#SharpeRatio(returns)
SharpeRatio.annualized(returns)
vol.annual = sapply(returns, FUN = function(x) sqrt(12)*sd(x))
print("Annualized Volatility:")
print(vol.annual)
maxDrawdown(returns)

end_time <- Sys.time()

time = end_time - start_time
