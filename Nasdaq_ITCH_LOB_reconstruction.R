###############################################################################
## Reconstruction of limit order book from the NASAQ TotalView ITCH 5.0 data
## This code is used to reconstruct the limit order book from the NASDAQ TotalView
## ITCH data. Before we start to reconstruct the limit order book, all messages
## are extracted from the RITCH package. 
##
## Author: Richie R. Ma; ruchuan2@illinois.edu, University of Illinois at
## Urbana-Champaign, 
##
## RITCH package: https://github.com/DavZim/RITCH
##
## NQTVITCHSpecification:
## https://m.nasdaqtrader.com/content/technicalsupport/specifications/dataproducts/NQTVITCHSpecification.pdf
###############################################################################



library(data.table)
library(RITCH)
library(nanotime)
library(doParallel)
rm(list=ls())






### Note that for some stocks, the trading symbol and the ticker are not the same
### According the CRSP, 
### "Trading Ticker Symbol is the trading symbol listed by exchanges and consolidated quote systems. 
### It includes all temporary values, share classes, and share type suffixes. 
### There is no punctuation (no periods) in the Trading Ticker Symbol. 
### NASDAQ symbol needs the punctuation.

load("C:/Users/ruchuan2/Box/odd_lot_quote_project/NASDAQ ITCH/filtered_sample.rda")

# stock.list <- sample[DATE==dates[i], unique(SYM_ROOT)]
# 
# sample[, nasdaq.sym:=fifelse(TSYMBOL==TICKER, TSYMBOL, paste0(TICKER, ".", substr(TSYMBOL, nchar(TSYMBOL), nchar(TSYMBOL))))]
# 
# save(sample, file = "C:/Users/ruchuan2/Box/odd_lot_quote_project/NASDAQ ITCH/filtered_sample.rda")


#----------------------Parse the NASDAQ binary itch file------------------------------------------

stock.list <- sample[, unique(nasdaq.sym)]
rm(sample)
gc()


all <- read_itch("C:/Users/ruchuan2/Box/odd_lot_quote_project/NASDAQ ITCH/10302019.NASDAQ_ITCH50.gz", 
                       c("system_events", "stock_directory","trading_status", "orders", "market_participant_states", "modifications", "trades"),
                       quiet = TRUE)



gc()


##### 
##### The reconstruction should be done by each stock.
#### For the test, we focus on the Apple Inc. as an example with Nasdaq code: AAPL


market.open <- all[["system_events"]][event_code=="Q", datetime]
market.close <- all[["system_events"]][event_code=="M", datetime]

date <- substr(market.close, 1, 10)
print(date)

stock.directory <- all[["stock_directory"]][stock %in% stock.list, unique(stock_locate)]
stock.directory <- all[["trading_status"]][stock_locate %in% stock.directory & "T" %in% trading_state, unique(stock_locate)]
# 
market.participants <- all[["market_participant_states"]][stock_locate %in% stock.directory]

save(market.participants, file=paste0('/projects/aces/ruchuan2/itch_book/market_participants/', date, ".rda"))
rm(market.participants)

gc()

trades <- all[["trades"]][stock_locate %in% stock.directory & datetime %between% c(market.open, market.close)]
save(trades, file=paste0('/projects/aces/ruchuan2/itch_book/trades/',date,".rda"))
rm(trades)
gc()

messages <- rbind(all[["orders"]][stock_locate %in% stock.directory], all[["modifications"]][stock_locate %in% stock.directory],fill=TRUE)
rm(all)
gc()

messages <- messages[, .(datetime, msg_type, stock_locate, order_ref, new_order_ref, buy, shares, price)]
gc()
setkey(messages, stock_locate, datetime)
gc()
rm(stock.list)
gc()

##------------------------message category--------------------------------------
## A: Limit order submission (order ID, price, size)
## F: Limit order submission with market participant ID (order ID, price, size, MPID)
## E: Limit order execution (order ID, execution size)
## C: Limit order execution with different prices (order ID, execution size)
## X: Limit order cancellation (partially) (order ID, cancellation size)
## D: Limit order deletion (totally) (order ID)
## U: Limit order replacement (original order ID, new order ID)
##
## The order replacement, cancellation, deletion, and execution are completely
## traced by the order ID, no stock symbol available.

## One should use stock_locate for the consistency, however, need to check
## this variable every day as the Nasdaq states that this variable is unchanged 
## in terms of intraday data.
##-----------------------------------------------------------------------------

## constructing an empty limit order book
## It is worth noting that Nasdaq ITCH data show all messages for all price 
## depths in the limit order book. Thus, the initial order book should be
## as large as it can. This codes start for 100 depths.

gc()


messages[, `:=`(order_ref=as.numeric(order_ref), new_order_ref=as.numeric(new_order_ref))]
stock.directory <- messages[, unique(stock_locate)]
registerDoParallel(cl <- makeCluster(3))

message.list <-list()

##for(j in 1:length(stock.directory)){

test <- foreach(j=1:2, .packages = c("data.table", "nanotime") ) %dopar% {
  message <- messages[stock_locate==stock.directory[j]]

 ## print(stock.directory[j])
  
##-------------------------------------------------------------------------------
## Some message do not have enough variables like the addition messages
## In this step, we need to fill these missing information for messages
## other than addition.


add <- message[msg_type=="A" | msg_type=="F"]

## modification messages

modification <- message[msg_type=="U"]

modification1 <- merge(message[msg_type=="U"], add[, .(order_ref, price, buy, shares)], by="order_ref")
modification1[, buy.x:=buy.y]
setnames(modification1, c("buy.x", "price.x", "shares.x"), c("buy", "price", "shares"))
setnames(modification1, c("price.y", "shares.y"), c("last.update.price", "last.update.shares"))
modification1 <- modification1[, -c("buy.y")]



modification2 <- merge(message[msg_type=="U"], 
                       modification[, .(new_order_ref,buy,shares,price)], 
                       by.x="order_ref",by.y="new_order_ref")

modification2[, buy.x:=buy.y]
setnames(modification2, c("buy.x", "price.x", "shares.x"), c("buy", "price", "shares"))
setnames(modification2, c("price.y", "shares.y"), c("last.update.price", "last.update.shares"))
modification2 <- modification2[, -c("buy.y")]

## assigning the bid/ask for the modification dataset

modification <- rbind(modification1,modification2)
rm(modification1, modification2)

repeat{
  
  
  modification <- merge(modification, 
                        modification[is.na(buy)==FALSE, .(new_order_ref,buy)], 
                        by.x="order_ref",by.y="new_order_ref", all.x = TRUE)
  modification[is.na(buy.x), buy.x:=buy.y]
  modification <- modification[, -c("buy.y")]
  setnames(modification, "buy.x", "buy")
  # print(modification[is.na(buy), .N])
  
  if(modification[is.na(buy), .N]==0){
    break
  }
  
}



#rm(modification1, modification2)

#### decompose the modification messages  (addition and deletion)
# 
# add.modification <- data.table(datetime=modification$datetime, msg_type="A", order_ref=modification$order_ref, 
#                    new_order_ref=modification$new_order_ref,buy=modification$buy, shares=modification$shares, price=modification$price)
# 
# deletion.modification <- data.table(datetime=modification$datetime, msg_type="D", order_ref=modification$order_ref, 
#                    new_order_ref=modification$new_order_ref,buy=modification$buy, shares=modification$last.update.shares, price=modification$last.update.price)

### execution messages

executions <- message[msg_type=="E"]
executions <- merge(executions, message[msg_type=="A" | msg_type=="F", .(order_ref, price, buy)], by="order_ref", all.x = TRUE)
executions[is.na(buy.x), `:=`(buy.x=buy.y,price.x=price.y)]
executions <- executions[, -c("buy.y", "price.y")]
setnames(executions, c("buy.x", "price.x"), c("buy", "price"))

### notice that some executions happen after the revision, so the order reference number does not work.
## try new_order_ref

executions <- merge(executions, modification[, .(new_order_ref, price, buy)], by.x="order_ref",by.y = "new_order_ref", all.x = TRUE)
executions[is.na(buy.x), `:=`(buy.x=buy.y,price.x=price.y)]
executions <- executions[, -c("buy.y", "price.y")]
setnames(executions, c("buy.x", "price.x"), c("buy", "price"))

### notice that the execution messages at different prices compared to the initial addition 
### include an implicit modifications.

executions2 <- message[msg_type=="C"]
executions2 <- merge(executions2, message[msg_type=="A" | msg_type=="F", .(order_ref, buy, price,shares)], by="order_ref", all.x = TRUE)
executions2[is.na(buy.x), `:=`(buy.x=buy.y)]
executions2 <- executions2[, -c("buy.y")]
setnames(executions2, c("buy.x","shares.x", "price.x", "price.y", "shares.y"), c("buy", "shares","price", "last.update.price","last.update.shares"))

executions2 <- merge(executions2, modification[, .(new_order_ref, buy, price,shares)], by.x="order_ref",by.y = "new_order_ref", all.x = TRUE)
executions2[is.na(buy.x), `:=`(buy.x=buy.y)]
executions2[is.na(last.update.price), `:=`(last.update.price=price.y)]
executions2[is.na(last.update.shares), `:=`(last.update.shares=shares.y)]
executions2 <- executions2[, -c("buy.y", "price.y", "shares.y")]
setnames(executions2, c("buy.x","shares.x", "price.x"), c("buy","shares", "price"))






# executions2.1 <- executions2[,.SD[.N>1], by=.(order_ref)][, `:=`(last.update.shares=0,last.update.price=0)][]
# executions2 <- merge(executions2, executions2.1[, .(order_ref, datetime, last.update.shares, last.update.price)], by=c("order_ref", "datetime"), all.x = TRUE)
# executions2[last.update.price.y==0, last.update.price.x:=NA]
# executions2[last.update.shares.y==0, last.update.shares.x:=NA]
# executions2 <- executions2[, -c("last.update.price.y", "last.update.shares.y")]
# setnames(executions2, c("last.update.price.x", "last.update.shares.x"),c("last.update.price", "last.update.shares"))
# 
# executions2.incE <- executions2[shares!=last.update.shares, order_ref]
# executions2.incE <- executions[order_ref %in% executions2.incE & msg_type=="E", order_ref]
# 
# executions2[order_ref%in%executions2.incE, last.update.shares:=shares]
# 
# rm(executions2.1)


# deletion.executions2 <- data.table(datetime=executions2[is.na(last.update.price)==F, datetime], msg_type="D", order_ref=executions2[is.na(last.update.price)==F, order_ref], 
#                                    new_order_ref=executions2[is.na(last.update.price)==F, new_order_ref],
#                                    buy=executions2[is.na(last.update.price)==F, buy], 
#                                    shares=executions2[is.na(last.update.price)==F, last.update.shares], 
#                                    price=executions2[is.na(last.update.price)==F, last.update.price])
# deletion.executions2[, order_seq_2:=2]
# 
# 
# addition.executions2 <- data.table(datetime=executions2[is.na(last.update.price)==F, datetime], msg_type="A", order_ref=executions2[is.na(last.update.price)==F, order_ref], 
#                                    new_order_ref=executions2[is.na(last.update.price)==F, new_order_ref],
#                                    buy=executions2[is.na(last.update.price)==F, buy], 
#                                    shares=executions2[is.na(last.update.price)==F, last.update.shares], 
#                                    price=executions2[is.na(last.update.price)==F, price])
# addition.executions2[, order_seq_2:=1]

# executions2.order.ref <- executions2[, unique(order_ref)]
executions <- rbind(executions, executions2, fill=TRUE)
rm(executions2)

#### dealing with cancellation and deletion messages
cancellation <- message[msg_type=="X"]

cancellation <- merge(cancellation,add[, .(order_ref, price, buy)], by="order_ref", all.x=TRUE)
cancellation[is.na(buy.x), `:=`(buy.x=buy.y,price.x=price.y)]
cancellation <- cancellation[, -c("buy.y", "price.y")]
setnames(cancellation, c("buy.x", "price.x"), c("buy", "price"))

cancellation <- merge(cancellation,modification[, .(new_order_ref, price, buy)], by.x="order_ref",by.y = "new_order_ref", all.x=TRUE)
cancellation[is.na(buy.x), `:=`(buy.x=buy.y,price.x=price.y)]
cancellation <- cancellation[, -c("buy.y", "price.y")]
setnames(cancellation, c("buy.x", "price.x"), c("buy", "price"))



deletion <- message[msg_type=="D"]

deletion <- merge(deletion, add[, .(order_ref, price, buy)], by="order_ref", all.x=TRUE)
deletion[is.na(buy.x), `:=`(buy.x=buy.y,price.x=price.y)]
deletion <- deletion[, -c("buy.y", "price.y")]
setnames(deletion, c("buy.x", "price.x"), c("buy", "price"))

deletion <- merge(deletion, modification[, .(new_order_ref, price, buy)], by.x="order_ref",by.y = "new_order_ref", all.x=TRUE)
deletion[is.na(buy.x), `:=`(buy.x=buy.y,price.x=price.y)]
deletion <- deletion[, -c("buy.y", "price.y")]
setnames(deletion, c("buy.x", "price.x"), c("buy", "price"))

# deletion.px <- executions[order_ref %in% executions2.order.ref & msg_type=="C", .SD[1], by=.(order_ref)]
# deletion <- merge(deletion, deletion.px[, .(order_ref, price)], by="order_ref", all.x=TRUE)
# deletion[is.na(price.y)==FALSE, price.x:=price.y]
# deletion <- deletion[, -c("price.y")]
# setnames(deletion, "price.x", "price")
# 
# rm(deletion.px)

message <- rbind(add, executions, modification, cancellation, deletion, fill=TRUE)


setkey(message, datetime)

#######
## Now, we calculating the deletion quantity. There are two patterns:
## 1) An order is originated from the plain addition message
## 2) An order has been modified

### An order is originated from the plain addition message

addition.delet <- message[msg_type=="D" & order_ref%in%add$order_ref, order_ref, by=.(order_ref)][, order_ref]


message[order_ref%in%addition.delet, shares:=fifelse(msg_type=="D", sum(shares[msg_type=="A"|msg_type=="F"])-
                                                       sum(shares[msg_type!="A" & msg_type!="F"& msg_type!="D"]), shares), by=.(order_ref)]

gc()


modifition.delet <- message[msg_type=="D" & order_ref%in%modification$new_order_ref, order_ref, by=.(order_ref)][, order_ref]

message[(order_ref%in%modifition.delet)|new_order_ref%in%modifition.delet, order_ref:=fifelse(msg_type=="U", new_order_ref, order_ref)]
message[(order_ref%in%modifition.delet), shares:=fifelse(msg_type=="D", sum(shares[msg_type=="U"])-
                                                       sum(shares[msg_type!="U"& msg_type!="D"]), shares), by=.(order_ref)]


print("message data processing completed")
rm(addition.delet, modifition.delet)
rm(add, cancellation, deletion, executions, modification)



lob.list <- list()


setkey(message, datetime)


for (i in 1:dim(message)[1]) {
  
 ## print(i)
  

  if(i>1){
    
    
      
      LOB <- lob.list[[i-1]]
   
    
  }else{
    
    LOB<- matrix(as.numeric(0), nrow=1, ncol=601)
  
  colnames(LOB)[c(1:601)] <- c("datetime",
                               "Bid_PX_100", "Bid_Qty_100", "Bid_Ord_100","Bid_PX_99", "Bid_Qty_99", "Bid_Ord_99",
                               "Bid_PX_98", "Bid_Qty_98", "Bid_Ord_98","Bid_PX_97", "Bid_Qty_97", "Bid_Ord_97",
                               "Bid_PX_96", "Bid_Qty_96", "Bid_Ord_96",
                               "Bid_PX_95", "Bid_Qty_95", "Bid_Ord_95","Bid_PX_94", "Bid_Qty_94", "Bid_Ord_94",
                               "Bid_PX_93", "Bid_Qty_93", "Bid_Ord_93","Bid_PX_92", "Bid_Qty_92", "Bid_Ord_92",
                               "Bid_PX_91", "Bid_Qty_91", "Bid_Ord_91",
                               "Bid_PX_90", "Bid_Qty_90", "Bid_Ord_90","Bid_PX_89", "Bid_Qty_89", "Bid_Ord_89",
                               "Bid_PX_88", "Bid_Qty_88", "Bid_Ord_88","Bid_PX_87", "Bid_Qty_87", "Bid_Ord_87",
                               "Bid_PX_86", "Bid_Qty_86", "Bid_Ord_86",
                               "Bid_PX_85", "Bid_Qty_85", "Bid_Ord_85","Bid_PX_84", "Bid_Qty_84", "Bid_Ord_84",
                               "Bid_PX_83", "Bid_Qty_83", "Bid_Ord_83","Bid_PX_82", "Bid_Qty_82", "Bid_Ord_82",
                               "Bid_PX_81", "Bid_Qty_81", "Bid_Ord_81",
                               "Bid_PX_80", "Bid_Qty_80", "Bid_Ord_80","Bid_PX_79", "Bid_Qty_79", "Bid_Ord_79",
                               "Bid_PX_78", "Bid_Qty_78", "Bid_Ord_78","Bid_PX_77", "Bid_Qty_77", "Bid_Ord_77",
                               "Bid_PX_76", "Bid_Qty_76", "Bid_Ord_76",
                               "Bid_PX_75", "Bid_Qty_75", "Bid_Ord_75","Bid_PX_74", "Bid_Qty_74", "Bid_Ord_74",
                               "Bid_PX_73", "Bid_Qty_73", "Bid_Ord_73","Bid_PX_72", "Bid_Qty_72", "Bid_Ord_72",
                               "Bid_PX_71", "Bid_Qty_71", "Bid_Ord_71",
                               "Bid_PX_70", "Bid_Qty_70", "Bid_Ord_70","Bid_PX_69", "Bid_Qty_69", "Bid_Ord_69",
                               "Bid_PX_68", "Bid_Qty_68", "Bid_Ord_68","Bid_PX_67", "Bid_Qty_67", "Bid_Ord_67",
                               "Bid_PX_66", "Bid_Qty_66", "Bid_Ord_66",
                               "Bid_PX_65", "Bid_Qty_65", "Bid_Ord_65","Bid_PX_64", "Bid_Qty_64", "Bid_Ord_64",
                               "Bid_PX_63", "Bid_Qty_63", "Bid_Ord_63","Bid_PX_62", "Bid_Qty_62", "Bid_Ord_62",
                               "Bid_PX_61", "Bid_Qty_61", "Bid_Ord_61",
                               "Bid_PX_60", "Bid_Qty_60", "Bid_Ord_60","Bid_PX_59", "Bid_Qty_59", "Bid_Ord_59",
                               "Bid_PX_58", "Bid_Qty_58", "Bid_Ord_58","Bid_PX_57", "Bid_Qty_57", "Bid_Ord_57",
                               "Bid_PX_56", "Bid_Qty_56", "Bid_Ord_56",
                               "Bid_PX_55", "Bid_Qty_55", "Bid_Ord_55","Bid_PX_54", "Bid_Qty_54", "Bid_Ord_54",
                               "Bid_PX_53", "Bid_Qty_53", "Bid_Ord_53","Bid_PX_52", "Bid_Qty_52", "Bid_Ord_52",
                               "Bid_PX_51", "Bid_Qty_51", "Bid_Ord_51",
                               "Bid_PX_50", "Bid_Qty_50", "Bid_Ord_50","Bid_PX_49", "Bid_Qty_49", "Bid_Ord_49",
                               "Bid_PX_48", "Bid_Qty_48", "Bid_Ord_48","Bid_PX_47", "Bid_Qty_47", "Bid_Ord_47",
                               "Bid_PX_46", "Bid_Qty_46", "Bid_Ord_46",
                               "Bid_PX_45", "Bid_Qty_45", "Bid_Ord_45","Bid_PX_44", "Bid_Qty_44", "Bid_Ord_44",
                               "Bid_PX_43", "Bid_Qty_43", "Bid_Ord_43","Bid_PX_42", "Bid_Qty_42", "Bid_Ord_42",
                               "Bid_PX_41", "Bid_Qty_41", "Bid_Ord_41",
                               "Bid_PX_40", "Bid_Qty_40", "Bid_Ord_40","Bid_PX_39", "Bid_Qty_39", "Bid_Ord_39",
                               "Bid_PX_38", "Bid_Qty_38", "Bid_Ord_38","Bid_PX_37", "Bid_Qty_37", "Bid_Ord_37",
                               "Bid_PX_36", "Bid_Qty_36", "Bid_Ord_36",
                               "Bid_PX_35", "Bid_Qty_35", "Bid_Ord_35","Bid_PX_34", "Bid_Qty_34", "Bid_Ord_34",
                               "Bid_PX_33", "Bid_Qty_33", "Bid_Ord_33","Bid_PX_32", "Bid_Qty_32", "Bid_Ord_32",
                               "Bid_PX_31", "Bid_Qty_31", "Bid_Ord_31",
                               "Bid_PX_30", "Bid_Qty_30", "Bid_Ord_30","Bid_PX_29", "Bid_Qty_29", "Bid_Ord_29",
                               "Bid_PX_28", "Bid_Qty_28", "Bid_Ord_28","Bid_PX_27", "Bid_Qty_27", "Bid_Ord_27",
                               "Bid_PX_26", "Bid_Qty_26", "Bid_Ord_26",
                               "Bid_PX_25", "Bid_Qty_25", "Bid_Ord_25","Bid_PX_24", "Bid_Qty_24", "Bid_Ord_24",
                               "Bid_PX_23", "Bid_Qty_23", "Bid_Ord_23","Bid_PX_22", "Bid_Qty_22", "Bid_Ord_22",
                               "Bid_PX_21", "Bid_Qty_21", "Bid_Ord_21",
                               "Bid_PX_20", "Bid_Qty_20", "Bid_Ord_20","Bid_PX_19", "Bid_Qty_19", "Bid_Ord_19",
                               "Bid_PX_18", "Bid_Qty_18", "Bid_Ord_18","Bid_PX_17", "Bid_Qty_17", "Bid_Ord_17",
                               "Bid_PX_16", "Bid_Qty_16", "Bid_Ord_16",
                               "Bid_PX_15", "Bid_Qty_15", "Bid_Ord_15","Bid_PX_14", "Bid_Qty_14", "Bid_Ord_14",
                               "Bid_PX_13", "Bid_Qty_13", "Bid_Ord_13","Bid_PX_12", "Bid_Qty_12", "Bid_Ord_12",
                               "Bid_PX_11", "Bid_Qty_11", "Bid_Ord_11",
                               "Bid_PX_10", "Bid_Qty_10", "Bid_Ord_10", "Bid_PX_9", "Bid_Qty_9", "Bid_Ord_9", "Bid_PX_8", "Bid_Qty_8", "Bid_Ord_8",
                               "Bid_PX_7", "Bid_Qty_7", "Bid_Ord_7", "Bid_PX_6", "Bid_Qty_6", "Bid_Ord_6", "Bid_PX_5", "Bid_Qty_5", "Bid_Ord_5",
                               "Bid_PX_4", "Bid_Qty_4", "Bid_Ord_4", "Bid_PX_3", "Bid_Qty_3", "Bid_Ord_3", "Bid_PX_2", "Bid_Qty_2", "Bid_Ord_2",
                               "Bid_PX_1", "Bid_Qty_1", "Bid_Ord_1",
                               "Ask_PX_1", "Ask_Qty_1", "Ask_Ord_1", "Ask_PX_2", "Ask_Qty_2", "Ask_Ord_2", "Ask_PX_3", "Ask_Qty_3", "Ask_Ord_3",
                               "Ask_PX_4", "Ask_Qty_4", "Ask_Ord_4", "Ask_PX_5", "Ask_Qty_5", "Ask_Ord_5", "Ask_PX_6", "Ask_Qty_6", "Ask_Ord_6",
                               "Ask_PX_7", "Ask_Qty_7", "Ask_Ord_7", "Ask_PX_8", "Ask_Qty_8", "Ask_Ord_8", "Ask_PX_9", "Ask_Qty_9", "Ask_Ord_9",
                               "Ask_PX_10", "Ask_Qty_10", "Ask_Ord_10", "Ask_PX_11", "Ask_Qty_11", "Ask_Ord_11", "Ask_PX_12", "Ask_Qty_12", "Ask_Ord_12",
                               "Ask_PX_13", "Ask_Qty_13", "Ask_Ord_13","Ask_PX_14", "Ask_Qty_14", "Ask_Ord_14", "Ask_PX_15", "Ask_Qty_15", "Ask_Ord_15",
                               "Ask_PX_16", "Ask_Qty_16", "Ask_Ord_16", "Ask_PX_17", "Ask_Qty_17", "Ask_Ord_17", "Ask_PX_18", "Ask_Qty_18", "Ask_Ord_18",
                               "Ask_PX_19", "Ask_Qty_19", "Ask_Ord_19","Ask_PX_20", "Ask_Qty_20", "Ask_Ord_20", "Ask_PX_21", "Ask_Qty_21", "Ask_Ord_21",
                               "Ask_PX_22", "Ask_Qty_22", "Ask_Ord_22", "Ask_PX_23", "Ask_Qty_23", "Ask_Ord_23", "Ask_PX_24", "Ask_Qty_24", "Ask_Ord_24",
                               "Ask_PX_25", "Ask_Qty_25", "Ask_Ord_25", "Ask_PX_26", "Ask_Qty_26", "Ask_Ord_26", "Ask_PX_27", "Ask_Qty_27", "Ask_Ord_27", 
                               "Ask_PX_28", "Ask_Qty_28", "Ask_Ord_28", "Ask_PX_29", "Ask_Qty_29", "Ask_Ord_29", "Ask_PX_30", "Ask_Qty_30", "Ask_Ord_30",
                               "Ask_PX_31", "Ask_Qty_31", "Ask_Ord_31",
                               "Ask_PX_32", "Ask_Qty_32", "Ask_Ord_32", "Ask_PX_33", "Ask_Qty_33", "Ask_Ord_33", "Ask_PX_34", "Ask_Qty_34", "Ask_Ord_34",
                               "Ask_PX_35", "Ask_Qty_35", "Ask_Ord_35", "Ask_PX_36", "Ask_Qty_36", "Ask_Ord_36", "Ask_PX_37", "Ask_Qty_37", "Ask_Ord_37", 
                               "Ask_PX_38", "Ask_Qty_38", "Ask_Ord_38", "Ask_PX_39", "Ask_Qty_39", "Ask_Ord_39", "Ask_PX_40", "Ask_Qty_40", "Ask_Ord_40",
                               "Ask_PX_41", "Ask_Qty_41", "Ask_Ord_41",
                               "Ask_PX_42", "Ask_Qty_42", "Ask_Ord_42", "Ask_PX_43", "Ask_Qty_43", "Ask_Ord_43", "Ask_PX_44", "Ask_Qty_44", "Ask_Ord_44",
                               "Ask_PX_45", "Ask_Qty_45", "Ask_Ord_45", "Ask_PX_46", "Ask_Qty_46", "Ask_Ord_46", "Ask_PX_47", "Ask_Qty_47", "Ask_Ord_47", 
                               "Ask_PX_48", "Ask_Qty_48", "Ask_Ord_48", "Ask_PX_49", "Ask_Qty_49", "Ask_Ord_49", "Ask_PX_50", "Ask_Qty_50", "Ask_Ord_50",
                               "Ask_PX_51", "Ask_Qty_51", "Ask_Ord_51",
                               "Ask_PX_52", "Ask_Qty_52", "Ask_Ord_52", "Ask_PX_53", "Ask_Qty_53", "Ask_Ord_53", "Ask_PX_54", "Ask_Qty_54", "Ask_Ord_54",
                               "Ask_PX_55", "Ask_Qty_55", "Ask_Ord_55", "Ask_PX_56", "Ask_Qty_56", "Ask_Ord_56", "Ask_PX_57", "Ask_Qty_57", "Ask_Ord_57", 
                               "Ask_PX_58", "Ask_Qty_58", "Ask_Ord_58", "Ask_PX_59", "Ask_Qty_59", "Ask_Ord_59", "Ask_PX_60", "Ask_Qty_60", "Ask_Ord_60",
                               "Ask_PX_61", "Ask_Qty_61", "Ask_Ord_61",
                               "Ask_PX_62", "Ask_Qty_62", "Ask_Ord_62", "Ask_PX_63", "Ask_Qty_63", "Ask_Ord_63", "Ask_PX_64", "Ask_Qty_64", "Ask_Ord_64",
                               "Ask_PX_65", "Ask_Qty_65", "Ask_Ord_65", "Ask_PX_66", "Ask_Qty_66", "Ask_Ord_66", "Ask_PX_67", "Ask_Qty_67", "Ask_Ord_67", 
                               "Ask_PX_68", "Ask_Qty_68", "Ask_Ord_68", "Ask_PX_69", "Ask_Qty_69", "Ask_Ord_69", "Ask_PX_70", "Ask_Qty_70", "Ask_Ord_70", 
                               "Ask_PX_71", "Ask_Qty_71", "Ask_Ord_71",
                               "Ask_PX_72", "Ask_Qty_72", "Ask_Ord_72", "Ask_PX_73", "Ask_Qty_73", "Ask_Ord_73", "Ask_PX_74", "Ask_Qty_74", "Ask_Ord_74",
                               "Ask_PX_75", "Ask_Qty_75", "Ask_Ord_75", "Ask_PX_76", "Ask_Qty_76", "Ask_Ord_76", "Ask_PX_77", "Ask_Qty_77", "Ask_Ord_77", 
                               "Ask_PX_78", "Ask_Qty_78", "Ask_Ord_78", "Ask_PX_79", "Ask_Qty_79", "Ask_Ord_79", "Ask_PX_80", "Ask_Qty_80", "Ask_Ord_80",
                               "Ask_PX_81", "Ask_Qty_81", "Ask_Ord_81",
                               "Ask_PX_82", "Ask_Qty_82", "Ask_Ord_82", "Ask_PX_83", "Ask_Qty_83", "Ask_Ord_83", "Ask_PX_84", "Ask_Qty_84", "Ask_Ord_84",
                               "Ask_PX_85", "Ask_Qty_85", "Ask_Ord_85", "Ask_PX_86", "Ask_Qty_86", "Ask_Ord_86", "Ask_PX_87", "Ask_Qty_87", "Ask_Ord_87", 
                               "Ask_PX_88", "Ask_Qty_88", "Ask_Ord_88", "Ask_PX_89", "Ask_Qty_89", "Ask_Ord_89", "Ask_PX_90", "Ask_Qty_90", "Ask_Ord_90",
                               "Ask_PX_91", "Ask_Qty_91", "Ask_Ord_91",
                               "Ask_PX_92", "Ask_Qty_92", "Ask_Ord_92", "Ask_PX_93", "Ask_Qty_93", "Ask_Ord_93", "Ask_PX_94", "Ask_Qty_94", "Ask_Ord_94",
                               "Ask_PX_95", "Ask_Qty_95", "Ask_Ord_95", "Ask_PX_96", "Ask_Qty_96", "Ask_Ord_96", "Ask_PX_97", "Ask_Qty_97", "Ask_Ord_97", 
                               "Ask_PX_98", "Ask_Qty_98", "Ask_Ord_98", "Ask_PX_99", "Ask_Qty_99", "Ask_Ord_99", "Ask_PX_100", "Ask_Qty_100", "Ask_Ord_100")
  
  }
 
  
  ##--------------------------- submission---------------------------------------
  
  if((message$msg_type[i]=="A")|(message$msg_type[i]=="F")){
    
   
    
    if(message$buy[i]=="TRUE"){
      

   
      ## outright bid orders
      px_seq <- LOB[,c(seq(2,299,3))]
      bid_index <- which(sort(unique(c(px_seq[px_seq>0],message$price[i])),decreasing = TRUE)==message$price[i])
      
      
      if(bid_index <= 100){
        
        
        bid_index_lv <- paste0("Bid_PX_",bid_index)
        bid_index_id <- which(colnames(LOB)==bid_index_lv)
        
        
        if(message$price[i] %in% px_seq==TRUE){
          
          
          
          
          LOB[,(bid_index_id+1)] <- message$shares[i]+LOB[,(bid_index_id+1)]
          LOB[,(bid_index_id+2)] <- LOB[,(bid_index_id+2)]+1
          
        }else{
          
          if(bid_index <100){
            
            
            
            LOB[, c(2:(bid_index_id-1))] <- LOB[, c(5:(bid_index_id+2))]
            
            
            
            
            LOB[,(bid_index_id)] <-  message$price[i]
            LOB[,(bid_index_id+1)] <- message$shares[i]
            LOB[,(bid_index_id+2)] <-  1
          } else{
            
            LOB[,(bid_index_id)] <-  message$price[i]
            LOB[,(bid_index_id+1)] <- message$shares[i]
            LOB[,(bid_index_id+2)] <-  1
            
          }
        }
        
      } 
      
    }
    
    if(message$buy[i]=="FALSE"){
      

      ## outright ask orders
      px_seq <- LOB[,c(seq(302,601,3))]
      ask_index <- which(sort(unique(c(px_seq[px_seq>0],message$price[i])),decreasing = FALSE)==message$price[i])
      
      if(ask_index <= 100){
        
        
        ask_index_lv <- paste0("Ask_PX_",as.character(ask_index))
        ask_index_id <- which(colnames(LOB)==ask_index_lv)
        
        
        if(message$price[i] %in% px_seq==TRUE){
        
          LOB[,(ask_index_id+1)] <- message$shares[i]+LOB[,(ask_index_id+1)]
          LOB[,(ask_index_id+2)] <-  LOB[,(ask_index_id+2)]+1
          
        }else{
          
          if(ask_index < 100){
            
            
            
            LOB[, c((ask_index_id+3):601)] <- LOB[, c(ask_index_id:598)]
            
            
            LOB[,(ask_index_id)] <-  message$price[i]
            LOB[,(ask_index_id+1)] <- message$shares[i]
            LOB[,(ask_index_id+2)] <-  1

          } else{
            LOB[,(ask_index_id)] <-  message$price[i]
            LOB[,(ask_index_id+1)] <- message$shares[i]
            LOB[,(ask_index_id+2)] <-  1

            
          }
          
        }
        
        
        
      }
      
      
      
    }
    
  }
  ##----------------------------- replacement --------------------------------
  
  
  
  if(message$msg_type[i]=="U"){
    
    # ## One needs to find the original addition message (A)
    # ## This could be done by a loop
    # 
    # order_ref_num <- message$order_ref[i]
    # 
    # ### one needs to find the last update
    # ### one also needs to find the original addition message to know the
    # ### order is at the bid or the ask side.
    # 
    # replace <- message[order_ref==order_ref_num]
    # last.update <-message[new_order_ref==order_ref_num]
    # 
    # #### the first modification
    # 
    # 
    # if(dim(last.update)[1]==0){
    #   
    #   first.add <- message[order_ref==order_ref_num & (msg_type=="A"|msg_type=="F")]
    #   message[msg_type=="U" & order_ref==order_ref_num, buy:=first.add$buy]
    #   last.update <- message[(msg_type=="A"|msg_type=="F") & order_ref==order_ref_num]
    # }
    # 
    ### one doesn't need to check the order side every time.
    ### You can update this when you finish one message.
    
    
    # repeat{
    # 
    # 
    #   first.add <- message[order_ref==order_ref_num]
    #   if("A" %in% first.add[, unique(msg_type)]|"F" %in% first.add[, unique(msg_type)]){
    # 
    #     break
    #   }else{
    #     first.add <- message[new_order_ref==order_ref_num]
    #   order_ref_num <- first.add[msg_type=="U", order_ref]
    #   #print(order_ref_num_2)
    # 
    # 
    #   }
    # 
    # }
    
    # message[msg_type=="U" & order_ref==order_ref_num, buy:=first.add$buy]
    qty.change <- message$shares[i]-message$last.update.shares[i]
    
    if(message$buy[i]=="TRUE"){

      px_seq <- LOB[,c(seq(2,299,3))] 
      
      if(message$price[i]==message$last.update.price[i]){  #-- replace order has the same price but different quantity  
        
        bid_index <- which(sort(unique(c(px_seq[px_seq>0],message$price[i])),decreasing = TRUE)==message$price[i])
        
        if(bid_index <=100){
          
          bid_index_lv <- paste0("Bid_PX_",as.character(bid_index))
          bid_index_id <- which(colnames(LOB)==bid_index_lv)
          
          
          if(message$price[i] %in% px_seq==TRUE) { #### the price level is still at the LOB
            
            if(qty.change <0){ ### only quantity is modified
              
              if((qty.change+LOB[,(bid_index_id+1)])>0){ ## decide if you need to remove one price level
                
                LOB[,(bid_index_id+1)] <- qty.change+LOB[,(bid_index_id+1)]
                
                
              }else{
                
                if(bid_index < 100){
                  LOB[, c(5:(bid_index_id+2))]   <- LOB[, c(2:(bid_index_id-1))]
                  LOB[,c(2:4)] <- 0
                  
                } else {
                  LOB[,c(2:4)] <- 0
                  
                }
                
              }
            } else{  
              ### increase the quantity
              
              LOB[,(bid_index_id+1)] <- qty.change+LOB[,(bid_index_id+1)]
              
              
            }
          }else { #### the price level is not at the LOB and needs to be created
            
            
            if(bid_index < 100){
              
              LOB[, c(2:(bid_index_id-1))] <- LOB[, c(5:(bid_index_id+2))] 
              
              LOB[,(bid_index_id)] <- message$price[i]
              LOB[,(bid_index_id+1)] <- message$shares[i]
              LOB[,(bid_index_id+2)] <-  1

              
            } else {
              
              LOB[,(bid_index_id)] <-  message$price[i]
              LOB[,(bid_index_id+1)] <-  message$shares[i]
              LOB[,(bid_index_id+2)] <-  1
            }
            
          }
        }
        
        
      }else{  #-- replace order does not have the same price 
        
        ### delete the original order
        
        if(message$last.update.price[i] %in% px_seq){
          
          bid_index.lag <- which(sort(unique(c(px_seq[px_seq>0],message$last.update.price[i])),decreasing = TRUE)==message$last.update.price[i])
          
          
          if(bid_index.lag <= 100){
            bid_index.lag_lv <- paste0("Bid_PX_",as.character(bid_index.lag))
            bid_index.lag_id <- which(colnames(LOB)==bid_index.lag_lv)
            
            if((LOB[, (bid_index.lag_id+1)]-message$last.update.shares[i])>0){
              
              LOB[,(bid_index.lag_id+1)] <- LOB[,(bid_index.lag_id+1)]-message$last.update.shares[i]
              LOB[,(bid_index.lag_id+2)] <- LOB[,(bid_index.lag_id+2)]-1
              
            }else{
              
              if(bid_index.lag < 100){
                LOB[, c(5:(bid_index.lag_id+2))]   <- LOB[, c(2:(bid_index.lag_id-1))]
                LOB[,c(2:4)] <- 0
                
              } else {
                LOB[,c(2:4)] <- 0
                
              }
              
            }
            
          }
          
          
        } 
        
        #### add replaced order
        
        px_seq <- LOB[,c(seq(2,299,3))]
        bid_index <- which(sort(unique(c(px_seq[px_seq>0],message$price[i])),decreasing = TRUE)==message$price[i])
        
        
        if(bid_index <= 100){
          
          bid_index_lv <- paste0("Bid_PX_",as.character(bid_index))
          bid_index_id <- which(colnames(LOB)==bid_index_lv)

          
          if(message$price[i] %in% px_seq==TRUE) { 
            
            LOB[,(bid_index_id+1)] <- message$shares[i]+LOB[,(bid_index_id+1)]
            LOB[,(bid_index_id+2)] <- LOB[,(bid_index_id+2)]+1
            
          }else{
            
            if(bid_index <100){
              
              LOB[, c(2:(bid_index_id-1))] <- LOB[, c(5:(bid_index_id+2))] ## backward move
              LOB[,bid_index_id] <-  message$price[i]
              LOB[,(bid_index_id+1)] <- message$shares[i]
              LOB[,(bid_index_id+2)] <-  1

              
            } else{
              
              LOB[,bid_index_id] <-  message$price[i]
              LOB[,(bid_index_id+1)] <- message$shares[i]
              LOB[,(bid_index_id+2)] <-  1
              
            }
            
          }
          
        }
        
        
        
        
        
      }
    }
    ######################################## 
    
    if( message$buy[i]=="FALSE"){
      

      
      px_seq <- LOB[,c(seq(302,601,3))]
      
      if(message$price[i]==message$last.update.price[i]){  
        
        ask_index <- which(sort(unique(c(px_seq[px_seq>0], message$price[i])),decreasing = FALSE)==message$price[i])
        
        
        
        if(ask_index <= 100){
          
          ask_index_lv <- paste0("Ask_PX_",as.character(ask_index))
          ask_index_id <- which(colnames(LOB)==ask_index_lv)
          
          if(message$price[i] %in% px_seq==TRUE) { 
            
            if(qty.change <0){
              
              if(qty.change+LOB[,ask_index_id+1]>0){
                
                
                LOB[,(ask_index_id+1)] <- qty.change+LOB[,(ask_index_id+1)]
                
                
              }else {
                
                if(ask_index < 100){
                  LOB[,c(ask_index_id:598)] <- LOB[, c((ask_index_id+3):601)]
                  LOB[,c(599:601)] <- 0
                  
                } else {
                  LOB[,c(599:601)] <- 0
                  
                }
                
              }
            }else{
              LOB[,(ask_index_id+1)] <- qty.change+LOB[,(ask_index_id+1)]
              
              
            }
          }else {
            
            
            if(ask_index < 100){
              LOB[, c((ask_index_id+3):601)] <- LOB[,  c(ask_index_id:598)] ## backward move
              LOB[,ask_index_id] <-   message$price[i]
              LOB[,(ask_index_id+1)] <- message$shares[i]
              LOB[,(ask_index_id+2)] <-  1

            } else {
              
              LOB[,ask_index_id] <-  message$price[i]
              LOB[,(ask_index_id+1)] <- message$shares[i]
              LOB[,(ask_index_id+2)] <-  1
            }
            
          }
        }
        
      }else{  #-- replace order does not have the same price 
        
        ### delete the original order
        
        if(message$last.update.price[i] %in% px_seq){
          
          ask_index.lag <- which(sort(unique(c(px_seq[px_seq>0],message$last.update.price[i])),decreasing = FALSE)==message$last.update.price[i])
          
          
          if(ask_index.lag <= 100){
            
            ask_index.lag_lv <- paste0("Ask_PX_",as.character(ask_index.lag))
            ask_index.lag_id <- which(colnames(LOB)==ask_index.lag_lv)
            
            
            if((LOB[, (ask_index.lag_id+1)]-message$last.update.shares[i])>0){
              
              LOB[,(ask_index.lag_id+1)] <- LOB[,(ask_index.lag_id+1)]-message$last.update.shares[i]
              
            }else{
              
              if(ask_index.lag < 100){
                LOB[,c(ask_index.lag_id:598)] <- LOB[, c((ask_index.lag_id+3):601)]
                LOB[,c(599:601)] <- 0
                
              } else {
                LOB[,c(599:601)] <- 0
                
              }
              
            }
            
          }
          
          
        } 
        
        #### add replaced order
        
        px_seq <- LOB[,c(seq(302,601,3))]
        ask_index <- which(sort(unique(c(px_seq[px_seq>0],message$price[i])),decreasing = FALSE)==message$price[i])
        
        
        if(ask_index <= 100){
          
          ask_index_lv <- paste0("Ask_PX_",as.character(ask_index))
          ask_index_id <- which(colnames(LOB)==ask_index_lv)
          
          

          if(message$price[i] %in% px_seq==TRUE) { 
            
            LOB[,(ask_index_id+1)] <- message$shares[i]+LOB[,(ask_index_id+1)]
            LOB[,(ask_index_id+2)] <- LOB[,(ask_index_id+2)]+1

            
          }else{
            
            if(ask_index <100){
              
              LOB[, c((ask_index_id+3):601)] <- LOB[,  c(ask_index_id:598)] ## backward move
              LOB[,ask_index_id] <-  message$price[i]
              LOB[,(ask_index_id+1)] <- message$shares[i]
              LOB[,(ask_index_id+2)] <- 1
            } else{
              
              LOB[,ask_index_id] <-  message$price[i]
              LOB[,(ask_index_id+1)] <- message$shares[i]
              LOB[,(ask_index_id+2)] <- 1

              
              
            }
            
          }
          
        }
        
        
        
        
        
      }
      
      
      
      
      
      
      
      
      
      
    }
  }
  
  ## -------------------------------- cancellation/deletion -------------------------------------------
  
  ## partial cancellation
  
  if(message$msg_type[i]=="X"){
    
    
    if(message$buy[i]=="TRUE"){
      

      px_seq <- LOB[,c(seq(2,299,3))]
      bid_index <- which(sort(unique(c(px_seq[px_seq>0],message$price[i])),decreasing = TRUE)==message$price[i])
      
      if(message$price[i] %in% px_seq==TRUE) { 
        
        
        if(bid_index <= 100){
          
          bid_index_lv <- paste0("Bid_PX_",as.character(bid_index))
          bid_index_id <- which(colnames(LOB)==bid_index_lv)
          
          ## test whether this depth needs to be removed
          
          if(LOB[,(bid_index_id+1)]-message$shares[i]>0){
            
            
            LOB[,(bid_index_id+1)] <- LOB[,(bid_index_id+1)]-message$shares[i]
            
            
          }else {
            
            if(bid_index < 100){
              LOB[,  c(5:(bid_index_id+2))]   <- LOB[,  c(2:(bid_index_id-1))]
              LOB[, c(2:4)] <- 0
              
            } else {
              LOB[, c(2:4)] <- 0
              
            }
            
          }
        }
      }
      
      
      
    }
    
    
    
    if(message$buy[i]=="FALSE"){
      
      px_seq <- LOB[,c(seq(302,601,3))]
      ask_index <- which(sort(unique(c(px_seq[px_seq>0],message$price[i])),decreasing = FALSE)==message$price[i])
      
      
      if(message$price[i] %in% px_seq==TRUE) { 
        
        
        if(ask_index <= 100){
          
          
          
          ask_index_lv <- paste0("Ask_PX_",as.character(ask_index))
          ask_index_id <- which(colnames(LOB)==ask_index_lv)
          
          if(LOB[,(ask_index_id+1)]-message$shares[i]>0){
            
            
            LOB[,(ask_index_id+1)] <- LOB[,(ask_index_id+1)]-message$shares[i]
            
            
          }else {
            
            if(ask_index < 100){
              LOB[,c(ask_index_id:598)] <- LOB[, c((ask_index_id+3):601)]
              LOB[,c(599:601)] <- 0
              
            } else {
              LOB[,c(599:601)] <- 0
              
            }
            
          }
        }
      }
      
      
      
    }
    
  }
  
  ### full deletion message  
  
  if(message$msg_type[i]=="D"){
    
    
    if(message$buy[i]=="TRUE"){
      

      px_seq <- LOB[,c(seq(2,299,3))]
      bid_index <- which(sort(unique(c(px_seq[px_seq>0],message$price[i])),decreasing = TRUE)==message$price[i])
      
      if(message$price[i] %in% px_seq==TRUE) { 
        
        
        if(bid_index <=100){
          
          bid_index_lv <- paste0("Bid_PX_",as.character(bid_index))
          bid_index_id <- which(colnames(LOB)==bid_index_lv)
          
          ## test whether this depth needs to be removed
          
          if(LOB[,(bid_index_id+1)]-message$shares[i]>0){
            
            
            LOB[,(bid_index_id+1)] <- LOB[,(bid_index_id+1)]-message$shares[i]
            LOB[,(bid_index_id+2)] <- LOB[,(bid_index_id+2)]-1
            
          }else {
            
            if(bid_index <100){
              LOB[, c(5:(bid_index_id+2))]   <- LOB[, c(2:(bid_index_id-1))]
              LOB[,c(2:4)] <- 0
              
            } else {
              LOB[,c(2:4)] <- 0
              
            }
            
          }
        }
      }
      
      
      
    }
    
    
    
    if(message$buy[i]=="FALSE"){
      

      px_seq <- LOB[,c(seq(302,601,3))]
      ask_index <- which(sort(unique(c(px_seq[px_seq>0],message$price[i])),decreasing = FALSE)==message$price[i])
      
      
      if(message$price[i] %in% px_seq==TRUE) { 
        
        
        if(ask_index <= 100){
          
          ask_index_lv <- paste0("Ask_PX_",as.character(ask_index))
          ask_index_id <- which(colnames(LOB)==ask_index_lv)
          
          if(LOB[,(ask_index_id+1)]-message$shares[i]>0){
            
            
            LOB[,(ask_index_id+1)] <- LOB[,(ask_index_id+1)]-message$shares[i]
            LOB[,(ask_index_id+2)] <-LOB[,(ask_index_id+2)]-1
            
          }else {
            
            if(ask_index < 100){
              LOB[,c(ask_index_id:598)] <- LOB[, c((ask_index_id+3):601)]
              LOB[,c(599:601)] <- 0
              
            } else {
              LOB[,c(599:601)] <- 0
              
            }
            
          }
        }
      }
      
      
      
    }
    
  }
  
  
  ## -------------------------------- execution ------------------------------------
  
  if(message$msg_type[i]=="E"){
    
    if(message$buy[i]=="TRUE"){
      

      px_seq <- LOB[,c(seq(2,299,3))]
      bid_index <- which(sort(unique(c(px_seq[px_seq>0],message$price[i])),decreasing = TRUE)==message$price[i])
      
      if(message$price[i] %in% px_seq==TRUE) { 
        
        
        if(bid_index <= 100){
          
          bid_index_lv <- paste0("Bid_PX_",as.character(bid_index))
          bid_index_id <- which(colnames(LOB)==bid_index_lv)
          
          ## test whether this depth needs to be removed
          
          if(LOB[,(bid_index_id+1)]-message$shares[i]>0){
            
            
            LOB[,(bid_index_id+1)] <- LOB[,(bid_index_id+1)]-message$shares[i]
            
            
          }else {
            
            if(bid_index < 100){
              LOB[, c(5:(bid_index_id+2))]   <-LOB[, c(2:(bid_index_id-1))]
              LOB[,c(2:4)] <- 0
              
            } else {
              LOB[,c(2:4)] <- 0
              
            }
            
          }
        }
      }
      
      
      
    }
    
    
    
    if(message$buy[i]=="FALSE"){

      
      px_seq <- LOB[,c(seq(302,601,3))]
      ask_index <- which(sort(unique(c(px_seq[px_seq>0],message$price[i])),decreasing = FALSE)==message$price[i])
      
      
      if(message$price[i] %in% px_seq==TRUE) { 
        
        
        if(ask_index <= 100){
          
          ask_index_lv <- paste0("Ask_PX_",as.character(ask_index))
          ask_index_id <- which(colnames(LOB)==ask_index_lv)
          
          
          if(LOB[,(ask_index_id+1)]-message$shares[i]>0){
            
            
            LOB[,(ask_index_id+1)] <- LOB[,(ask_index_id+1)]-message$shares[i]
            
            
          }else {
            
            if(ask_index < 100){
              LOB[,c(ask_index_id:598)] <- LOB[, c((ask_index_id+3):601)]
              LOB[,c(599:601)] <- 0
              
            } else {
              LOB[,c(599:601)] <- 0
              
            }
            
          }
        }
      }
      
      
      
    }
    
  }
  
  
  
  if(message$msg_type[i]=="C"){
    
    if(message$buy[i]=="TRUE"){ 

      px_seq <- LOB[,c(seq(2,299,3))]

      
      if(message$last.update.price[i] %in% px_seq){
        
        bid_index.lag <- which(sort(unique(c(px_seq[px_seq>0],message$last.update.price[i])),decreasing = TRUE)==message$last.update.price[i])
        
        
        if(bid_index.lag <= 100){
          
          bid_index.lag_lv <- paste0("Bid_PX_",as.character(bid_index.lag))
          bid_index.lag_id <- which(colnames(LOB)==bid_index.lag_lv)
          
          if((LOB[, (bid_index.lag_id+1)]-message$shares[i])>0){
            
            LOB[,(bid_index.lag_id+1)] <- LOB[,(bid_index.lag_id+1)]-message$shares[i]

            
          }else{
            
            if(bid_index.lag < 100){
              LOB[, c(5:(bid_index.lag_id+2))]   <-LOB[, c(2:(bid_index.lag_id-1))]
              LOB[,c(2:4)] <- 0
              
            } else {
              LOB[,c(2:4)] <- 0
              
            }
            
          }
          
        }
        
        
      } 
      

    }
    
    
    
    if(message$buy[i]=="FALSE"){ 

      px_seq <- LOB[,c(seq(302,601,3))]
      
      
      if(message$last.update.price[i] %in% px_seq){
        
        ask_index.lag <- which(sort(unique(c(px_seq[px_seq>0],message$last.update.price[i])),decreasing = FALSE)==message$last.update.price[i])
        
        
        if(ask_index.lag <= 100){
          
          ask_index.lag_lv <- paste0("Ask_PX_",as.character(ask_index.lag))
          ask_index.lag_id <- which(colnames(LOB)==ask_index.lag_lv)
          
          
          if((LOB[, (ask_index.lag_id+1)]-message$shares[i])>0){
            
            LOB[,(ask_index.lag_id+1)] <- LOB[,(ask_index.lag_id+1)]-message$shares[i]
            
          }else{
            
            if(ask_index.lag < 100){
              LOB[,c(ask_index.lag_id:598)] <- LOB[, c((ask_index.lag_id+3):601)]
              LOB[,c(599:601)] <- 0
            
            } else {
              LOB[,c(599:601)] <- 0
              
            }
            
          }
          
        }
        
        
      } 
      
     
    
      
      
   
      
     

      
      
    }
    
  }
  
  
 lob.list[[i]] <- LOB
  
  
}

lob.list <- lapply(lob.list, function(x){x[, c(299:304)]})
book <- as.data.table(data.table::transpose(lob.list), col.names = names(lob.list[[1]]))
colnames(book) <- names(lob.list[[1]])

book[, datetime:=message$datetime]
book <- book[datetime %between% c(market.open, market.close)]

setcolorder(book, c(7, 1:6))

rm(lob.list, LOB)

gc()

print("LOB completed")

message <- message[datetime %between% c(market.open, market.close)]
message <- cbind(message, book[, .(Bid_PX_1, Ask_PX_1)])



}

messages <- rbindlist(message.list)
save(messages, file=paste0('/projects/aces/ruchuan2/itch_book/messages/', date, '.rda'))

