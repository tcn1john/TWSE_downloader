library("ggplot2")
require(compiler)
library(foreach)
library(stringr)
library("dplyr")

startdate="20190801"

parallel_stock=1
parallel_ncpu=1

enableJIT(3)
Sys.setlocale(category = "LC_ALL", locale = "cht")

list_stock=(0:9999)
list_stock=c("2498","2353")

###constants
name_col_1=c("X","date","open","high","low","close")
name_col_2=c("X","date","v_units","v_ntd","open","high","low","close","delta_price","deal_orders")
name_col_3=c("date","open","high","low","close")
name_col_4=c("date","code","stock_name","deal_unit","deal_orders","deal_ntd","open","high","low","close","pn","delta","last_buy_price","last_buy_quan","last_sell_price","last_sell_quan","PE")
name_col=list(name_col_1,name_col_2,name_col_3,name_col_4)
col_date <- 2

read_single_csv <-function(filename){
  if(file.size(filename)<5)
    return(FALSE)
  ###ELSE CONTINUE
  df=read.csv(filename,header=F,fill=T,sep=",",col.names=c(1:19),skip=100)

  while( !grepl(df[1,1],pattern="00[0-9]+") ){
    df <- df[-1,] 
  }
  for (i in c(ncol(df):1) ){
    if(is.weird(df[20,i])){
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

to.bool <- function (x){
  if( is.na(x) ){
    return(F) 
  } else if (length(x)==0){
    return(F)
  }
  else if( nchar(x)==0 ){
    return(F) 
  } else {
    return(x) 
  }
}
is.weird <- function (x){
  if( length(x)==0 ) {
    return(T) 
  }else if( is.na(x) | is.infinite(x) ) {
    return(T) 
  }else if( x==""){
    return(T) 
  }else{
    return(F) 
  }
}

cat("Reading path... \n")
getwd()
cat("Reading path from rstudio... \n")
this.dir <- dirname(parent.frame(2)$ofile)
setwd(this.dir)
filelist <- list.files(path="./daily_rawdata/","twstock_[0-9]{4,8}.csv")
setwd("./daily_rawdata/")
cat("Reading path done \n")

### Remove small files (0 size almost)
cat("Remove small files... \n")
for(fn_analy in filelist){
  if(file.size(fn_analy)<10){
    file.remove(fn_analy)   
  }
}
cat("Remove small files done \n")

###check start date....
filelist_bind <- list.files("./","^bind_[0-9]+")
i <- 1
while (  i<=10 ){
  filename_bind <- filelist_bind[ as.integer( runif(1) * length(filelist_bind) ) ]
  Lskip <- as.integer(( file.size(filename_bind) / 200 ) -10)
  df <- tail(read.csv(filename_bind,skip=Lskip),n = 1)
  if( as.numeric(df[1,2]) >= as.numeric(startdate) ){
    cat("\nConflict dates: ",as.numeric(df[1,2]), as.numeric(startdate),"\n")
    choice_date <- readline(prompt="Check start date. Use auto start date? (y/n):") 
    if( tolower(choice_date)=="y" ){
      startdate <- as.numeric(df[1,2]) +1
    }
  }
  i <- i+1
}
cat( "Start date for use:",startdate,"\n")

### exclude files before startdate
filelist <- list.files(,"twstock_[0-9]{4,8}.csv")
filelist <- filelist[str_extract( filelist,"[0-9]+" ) >= startdate]


df_allstock=list()
df=as.data.frame("")
### renew stock list? or preset
readline(prompt="Read all stock from latest list of stocks (y/n)?  :") -> option_read
if( option_read!="n" ){
  cat("Reading...\n")
  df <- read_single_csv(filelist[length(filelist)])   
  list_stock <- df[,2][grep(x =  df[,2],pattern="[0-9]{4}" )]
}
n_stock <- length(list_stock) / parallel_ncpu
list_stock <- list_stock [ as.integer(1+n_stock*(parallel_stock-1)):as.integer(n_stock*(parallel_stock)) ]


### define append function
cat("Define functions...\n")
append_to_stocklist <- function (df_stockn,target_stock){
  df_stockn <- rbind(df_stockn, df[df[,2]==target_stock,])
  return(df_stockn)
}
append_to_stocklist <- cmpfun(append_to_stocklist)

append_stock_lapply <- function ( n_stock){
  return(append_to_stocklist(df_allstock[[n_stock]],list_stock[n_stock]))
}
append_stock_lapply <- cmpfun(append_stock_lapply)

cat("Start combine...\n")
n_loop <- 1
df_allstock=list()
###slow method
for ( fn_analy in filelist ){
  n_stock=1
  df <- read_single_csv(fn_analy)
  
  if( fn_analy == filelist[1] | length(df_allstock)==0 ){
    df_allstock=list()
    for ( n_stock in (1:length(list_stock)) ){
      target_stock <- list_stock[n_stock]
      df_allstock[[n_stock]]      <- df[df[,2]==target_stock,]
    }
  }
  
  df_allstock <- lapply( (1:length(list_stock)), append_stock_lapply)
  
  ##df_allstock[[i]] <- foreach(i=1:length(list_stock)) %dopar% {
  ##  append_stock_lapply(i)
  ##}
  
  if ( n_loop %% 10 ==0 | fn_analy==tail(filelist,1) ){
    for (i_print in (1:length(df_allstock)) ){
      
      #write.csv(df_allstock[[i_print]],file = paste("bind_",i_print,".csv" ),append=T)
      #lastdate <- max(na.omit(read.csv( file = paste("bind_",i_print,".csv" ), header = F)[,2]))
      #newdate <- min(as.numeric(as.character(tail(df_allstock[[i_print]][,1]))))
      #if(nchar(lastdate)>4) if( newdate <= lastdate ){
      # cat("Please check start date. The existing data is very new. \n" )
      #  cat("last date in data:",lastdate,"\tthe date of incoming data:",newdate,"\n")
      #  cat("exiting...\n")
      #  stop()
      #}else{
        write.table(df_allstock[[i_print]][1,],file = paste("bind_",df_allstock[[i_print]][1,2],".csv",sep="" ), sep = ",",append=T,col.names = F)
      #}
      
      
    }
    df_allstock <- list()
  }
  n_loop <- n_loop+1
}

readline(prompt="Re-scan for duplicate date in all csv? (y/n):") -> option_read
if ( 1 ){
  filelist <- list.files("./","^bind_[0-9]+")
  for ( fn_analy in filelist ){
    n_stock=c()
    df <- read.csv(fn_analy)
    if(tolower(option_read)=="y"){
      for ( i in (1:(nrow(df)-1)) ){
        if (    is.weird(df[i,2]) | is.weird(df[i,3])   ){
          n_stock <- c(n_stock,(i))
        }else if( is.weird(df[(i+1),2]) | is.weird(df[(i+1),3]) ){
          n_stock <- c(n_stock,(i+1))
        }else if( df[i,2]==df[(i+1),2] & df[i,3]==df[(i+1),3] ){
           n_stock <- c(n_stock,i)
        }else if ( df[(i+1),2] =="" & df[(i+1),3] == "" ){
          n_stock <- c(n_stock,(i+1))
        }
      }
    }
    if( !is.weird(n_stock) )
      df <- df[-(n_stock),]
    df <- df[,(-1)]
    colnames(df) <- name_col_4
    df <- distinct(df,df[,1],.keep_all = T)
    write.csv(x = df,file=fn_analy,row.names = T )
  }
} 
