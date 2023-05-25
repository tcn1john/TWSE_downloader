library("ggplot2")
require(compiler)
enableJIT(3)
Sys.setlocale(category = "LC_ALL", locale = "cht")

###SWITCHES
switch_download=1     ###c(-1,0,1)
switch_createcsv=1
switch_latest_refresh=1

###flags
flags_downloadcooling=0

###Date range
date_start="2019-08-01"
date_end=Sys.Date()
N_rows_per_file=50000

getwd()
this.dir <- dirname(parent.frame(2)$ofile)
###setwd("G:/Lab_Rplotting/Stock")
setwd(this.dir)

###constants
name_col_1=c("X","date","open","high","low","close")
name_col_2=c("X","date","v_units","v_ntd","open","high","low","close","delta_price","deal_orders")
name_col_3=c("date","open","high","low","close")
name_col_4=c("date","v_units","v_ntd","open","high","low","close","delta_price","deal_orders")
name_col=list(name_col_1,name_col_2,name_col_3,name_col_4)

nnrow <-function(x){
   if(length(x)){
    return(nrow(x)) 
   }else{
    return(0) 
   }
}

Rsleep <- function(N=runif(1,5,10)){
  print(N)
  Sys.sleep(N)
}

###Create date reange
date_range <- function(){
  return(gsub("-","",seq(from=as.Date(date_start), to=as.Date(date_end), by=1)))
}
date=date_range()
cat("Date range is:",date[1],", to",date[length(date)],"\n")

read_single_csv <-function(filename){
  if(file.size(filename)<5)
    return(FALSE)
  ###ELSE CONTINUE
  df=read.csv(filename,header=F,fill=T,sep=",",col.names=c(1:19))
  for (i in c(ncol(df):1) ){
    if(is.na(df[20,i])){
      df=df[,-i]
    }
  }
  colnames(df)=df[20,]
  colnames(df)=c("code","name",
                 "v_units","v_orders","v_ntd",
                 "open","high","low","close",
                 "delta_sign","delta_price",
                 "final_buy_price","final_buy_v",
                 "final_sell_price","final_sell_v",
                 "P/E")
  df$code=gsub("=","",df$code)
  df=cbind( 
    substr(filename,gregexpr("twstock_",filename)[[1]][1]+8,gregexpr("csv",filename)[[1]][1]-2),
    df)
  colnames(df)[1]="date" 
  print(filename)
  return(df)
}
read_single_csv <- cmpfun(read_single_csv)

read_consolidated_csv <-function(filename,stock=2498){
  df=read.csv(filename)
  return(df[which(    (as.numeric(df$code)<100)    ),])
}
read_consolidated_csv<-cmpfun(read_consolidated_csv)
###SAMPLE URL
###http://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=20190308&type=ALLBUT0999

url_a="http://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date="
url_b="&type=ALLBUT0999"
url=paste(url_a,date[1],url_b,sep="")

###ASK IF REMOVE LATEST CONSOLIDATED FILE

if(switch_latest_refresh){
  FL_consolidated=list.files(,"__stock__twstock_")
  if( max(as.numeric(gsub("[^0-9]","",FL_consolidated))) == as.numeric(date[length(date)]) )
    file.remove(FL_consolidated[which.max(as.numeric(gsub("[^0-9]","",FL_consolidated)))])
}
  

N_count_i=0
df_stock={}
for (i in date){
  N_count_i=N_count_i+1
  t_start=Sys.time()
  
  ###DOWNLOAD PROCESS IN THIS IF
  if(switch_download){
    url=paste(url_a,i,url_b,sep="")
    filename=paste("twstock_",i,".csv",sep="")
    if (!file.exists(filename)){    
      print(filename)
      download.file(url,filename,quiet=T)
      flags_downloadcooling=1
    }
  }
  
  ###CONSOLIDATE PROCESS IN THIS IF
  if(switch_createcsv ){
    if(nnrow(df_stock)>N_rows_per_file || i==date[1] || i==date[length(date)] ){
      if(nnrow(df_stock)>N_rows_per_file || i==date[length(date)] )
        write.csv(x=df_stock,file = paste("__stock__",filename,sep=""))
      df_stock={}
      df={}
      FL_consolidated=list.files(,"__stock__twstock_")
      FL_consolidated=max(as.numeric(gsub("[^0-9]","",FL_consolidated)))
    }
    if(file.size(filename)>10 && i>FL_consolidated ){
      df=read_single_csv(filename)
      if( length(df_stock)==0 ){
        df_stock=df
      }else{
        df_stock=rbind(df,df_stock) 
      }
    }
  }
  t_end=Sys.time()
  if(t_end-t_start<5 && flags_downloadcooling!=0){
    flags_downloadcooling=0
    Rsleep()
  }
  
}

###DOWNLOADING ENDS HERE####
###DOWNLOADING ENDS HERE####

FL_daily <- list.files("./daily_rawdata","twstock")
FL <- list.files("./","^twstock_.+csv")
###clean the daily_rawdata folder
for (i in FL_daily){
  if(file.size(paste("./daily_rawdata/",i,sep="")) <5){
    cat("\tRemoving empty file ",i, " of daily_rawdata folder...\n")
    file.remove( (paste("./daily_rawdata/",i,sep=""))  )
  }
}

###copy data to working dir: daily_rawdata
for ( i in FL ){
  if( !file.exists( paste("./daily_rawdata/",i,sep="") ) ) if (  5 < file.size(i) ){
    file.copy(from = i, to= paste("./daily_rawdata/",i,sep="") )
    cat("\tCopying ",i, " to daily_rawdata folder...\n")
  }
}
