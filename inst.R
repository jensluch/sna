rm(list=ls())
options(RCurlOptions = list(cainfo = system.file("CurlSSL", "cacert.pem", package = "RCurl")))
#require(rjson)

Sys.setlocale("LC_ALL", "uk_UA.UTF-8")  


require(RCurl)
require(rjson)
require(RJDBC)
drv <- JDBC("com.vertica.jdbc.Driver", "/home/jensluch/www/vertica-jdk5-6.1.3-0.jar")
conn <- dbConnect(drv, "jdbc:vertica://10.11.5.21:5433/dwhdb", "phil", "Qq12345678")


#resp <- SERVER$args
#token <- (unlist((strsplit((unlist(strsplit(resp,'&'))),"="))[1]))[2]
token<-"464684905.2c52b84.3a1832fc5a5d449fa5678ee959ba2509"
client_id<-"2c52b843609d4477bee98b272d6fee57"
client_secret<-"359ca2ec870944658e1c85a8fb6ac929"
redirect_url<-"http://192.168.13.178/"
embed<-"http://api.instagram.com/oembed?url="
url<-"http://instagram.com/p/clmcQavflY/"
id<- "self" #"207323000" #"464684905" # "307266182"

options<-c("follows","followed-by","","media/recent","likes","feed", "media/liked")
sign<-""
methods<-c("users", "locations", "media", "geographies", "tags","")

inst <-  function(method, id="", option="", token,param=""){
    json_data <- getURL(sprintf("https://api.instagram.com/v1/%s/%s/%s?access_token=%s%s",method, id, option, token,param))
    data<-fromJSON(json_data,  unexpected.escape="keep")
}

#####inst user/get
user<-inst(method="users",id="self",token=token)
user$data$counts$media
userid<-user$data$id
b<-paste(user$data$id,user$data$username,user$data$full_name,user$data$website,
         user$data$bio,user$data$profile_picture,user$data$counts$media,
         user$data$counts$followed_by,user$data$counts$follows,sep="','")
qry<-sprintf("INSERT INTO sna.inst_users VALUES('%s');",b)

tryCatch({
    dbSendUpdate(conn=conn,statement=qry)
}, error=function(e){
    #error.log <- NULL
    error.log <- as.matrix(read.csv(file="/home/jensluch/www/inst_err_log.csv"))
    error.log <- rbind(error.log, c (conditionMessage(e),qry,nchar(qry)))
    write.csv(error.log, file="/home/jensluch/www/inst_err_log.csv", row.names=FALSE)
})



#######inst_user_liked
dat<-inst(method=methods[1],id=id,option=options[7],token=token)
N<-length(dat$data)
liked<-NULL
liked<-matrix(0,N,12)
i<-1

for(i in 1:N){
    tags<-""
    if(length(dat$data[[i]]$users_in_photo)==0){dat$data[[i]]$users_in_photo<-""}
    if(length(dat$data[[i]]$tags!=0)){
        for(k in 1:length(dat$data[[i]]$tags)){tags<-sprintf("%s %s",tags, dat$data[[i]]$tags[k])}}
    liked[i,1]<-format(tags, na.encode = TRUE)
    if(length(dat$data[[i]]$location)!=0){
        liked[i,2]<-format(dat$data[[i]]$location$name, na.encode = TRUE)
        liked[i,12]<-format(sprintf("%s %s",dat$data[[i]]$location$latitude,dat$data[[i]]$location$longitude), na.encode = TRUE)}
    liked[i,3]<-format(dat$data[[i]]$comments$count, na.encode = TRUE)
    liked[i,4]<-format(dat$data[[i]]$created_time, na.encode = TRUE)
    liked[i,5]<-format(dat$data[[i]]$link, na.encode = TRUE)
    liked[i,6]<-format(dat$data[[i]]$caption$text, na.encode = TRUE)
    liked[i,7]<-format(dat$data[[i]]$user$username, na.encode = TRUE)
    liked[i,8]<-format(dat$data[[i]]$user$id, na.encode = TRUE)
    liked[i,9]<-format(dat$data[[i]]$user$bio, na.encode = TRUE)
    liked[i,10]<-format(dat$data[[i]]$id, na.encode = TRUE)
    liked[i,11]<-format(dat$data[[i]]$users_in_photo, na.encode = TRUE)
}
liked.df<-as.data.frame(liked)
names<-c("tags","location_name","comments_count","created_time","link","Caption_text","username","id","bio","media_id","Users_in_photo","latitude longitude")
colnames(liked.df)<-names
#write.csv(liked.df, file = "Dropbox/BitBank/new/R/instagram/inst_liked.csv", na = 'NA')


#############inst_followed-by
dat<-inst(method=methods[1],id=id,option=options[2],token=token)         #,token="464684905.2c52b84.3a1832fc5a5d449fa5678ee959ba2509")
N<-length(dat$data)
followed<-NULL
followed<-matrix(0,N,6)
i<-1
for(i in 1:N){
    followed[i,1]<-format(dat$data[[i]]$username,na.encode = TRUE)
    followed[i,2]<-format(dat$data[[i]]$bio,na.encode = TRUE)
    followed[i,3]<-format(dat$data[[i]]$website,na.encode = TRUE)
    followed[i,4]<-format(dat$data[[i]]$profile_picture,na.encode = TRUE)
    followed[i,5]<-format(dat$data[[i]]$full_name,na.encode = TRUE)
    followed[i,6]<-format(dat$data[[i]]$id,na.encode = TRUE)}
followed.df<-as.data.frame(followed)

names<-c("username", "bio", "website", "profile_picture", "full_name",  "id")
colnames(followed.df) <- names
#insert into db
a<-paste(userid, followed.df$username,followed.df$full_name,followed.df$website,gsub("'","",followed.df$bio),followed.df$profile_picture,2,followed.df$id,sep="','")
qry<-sprintf("INSERT INTO sna.inst_friends VALUES('%s');",a)
for(i in 1:length(qry)){
    tryCatch({
        dbSendUpdate(conn=conn,statement=qry[i])
    }, error=function(e){
        #error.log <- NULL
        error.log <- as.matrix(read.csv(file="/home/jensluch/www/inst_err_log.csv"))
        error.log <- rbind(error.log, c (conditionMessage(e),qry,nchar(qry)))
        write.csv(error.log, file="/home/jensluch/www/inst_err_log.csv", row.names=FALSE)
    })
}

#write.csv(followed.df, file = "Dropbox/BitBank/new/R/instagram/inst_followed.csv", na = 'NA')


###############inst_follows
dat<-inst(method=methods[1],id=id,option=options[1],token=token)         #,token="464684905.2c52b84.3a1832fc5a5d449fa5678ee959ba2509")
N<-length(dat$data)
follows<-NULL
follows<-matrix(0,N,6)
i<-1
for(i in 1:N){
    follows[i,1]<-format(dat$data[[i]]$username,na.encode = TRUE)
    follows[i,2]<-format(dat$data[[i]]$bio,na.encode = TRUE)
    follows[i,3]<-format(dat$data[[i]]$website,na.encode = TRUE)
    follows[i,4]<-format(dat$data[[i]]$profile_picture,na.encode = TRUE)
    follows[i,5]<-format(dat$data[[i]]$full_name,na.encode = TRUE)
    follows[i,6]<-format(dat$data[[i]]$id,na.encode = TRUE)}
follows.df<-as.data.frame(follows)

names<-c("username", "bio", "website", "profile_picture", "full_name",  "id")
colnames(follows.df) <- names
#write.csv(follows.df, file = "Dropbox/BitBank/new/R/instagram/inst_follows.csv", na = 'NA')

#insert into db
a<-paste(userid, follows.df$username,follows.df$full_name,follows.df$website,gsub("'","",follows.df$bio),follows.df$profile_picture,1,follows.df$id,sep="','")
qry<-sprintf("INSERT INTO sna.inst_friends VALUES('%s');",a)
for(i in 1:length(qry)){
    tryCatch({
        dbSendUpdate(conn=conn,statement=qry[i])
    }, error=function(e){
        #error.log <- NULL
        error.log <- as.matrix(read.csv(file="/home/jensluch/www/inst_err_log.csv"))
        error.log <- rbind(error.log, c (conditionMessage(e),qry,nchar(qry)))
        write.csv(error.log, file="/home/jensluch/www/inst_err_log.csv", row.names=FALSE)
    })

}

##############inst_recent_media
jdata<-inst(method=methods[1],id=id,option=options[4],token=token,param="&count=-1")
N<-length(jdata$data)
feed<-NULL
i<-1
for(i in 1:N){if(length(jdata$data[[i]]$caption$text)==0) {jdata$data[[i]]$caption$text<-""}}
feed<-matrix(0,N,13)

likes_cnt<-0
for(i in 1:N){
    feed[i,1]<-format(jdata$data[[i]]$user$id, na.encode = TRUE)
    feed[i,2]<-format(jdata$data[[i]]$user$username, na.encode = TRUE)
    feed[i,3]<-format(jdata$data[[i]]$location$latitude, na.encode = TRUE)
    feed[i,4]<-format(jdata$data[[i]]$location$longitude, na.encode = TRUE)
    feed[i,5]<-format(jdata$data[[i]]$created_time, na.encode = TRUE)
    feed[i,6]<-format(jdata$data[[i]]$likes$count, na.encode = TRUE)
    feed[i,7]<-format(jdata$data[[i]]$link, na.encode = TRUE)
    feed[i,8]<-format(jdata$data[[i]]$filter, na.encode = TRUE)
    feed[i,9]<-format(jdata$data[[i]]$caption$text[1], na.encode = TRUE)
    feed[i,10]<-format(jdata$data[[i]]$type, na.encode = TRUE)
    feed[i,11]<-format(jdata$data[[i]]$id, na.encode = TRUE)
    feed[i,12]<-format(jdata$data[[i]]$user_has_liked,na.encode = TRUE)
    feed[i,13]<-format(jdata$data[[i]]$location$name, na.encode= T)
    likes_cnt<-likes_cnt+jdata$data[[i]]$likes$count
}
feed.df<-as.data.frame(feed)
names<-c("userid", "USERNAME", "Location_latitude", "location_longtitude", "created_time", "Likes_count","link","filter","caption","type","media_id","likes","location_name")
colnames(feed.df)<-names
a<-paste(feed.df$userid,feed.df$Location_latitude,feed.df$location_longtitude,
         feed.df$location_name,feed.df$created_time,feed.df$Likes_count,feed.df$link,feed.df$filter,feed.df$caption,feed.df$type,feed.df$media_id,sep="','")
qry<-sprintf("INSERT INTO sna.inst_recent VALUES('%s');",a)
for(i in 1:length(qry)){
    tryCatch({
        dbSendUpdate(conn=conn,statement=qry[i])
    }, error=function(e){
        #error.log <- NULL
        error.log <- as.matrix(read.csv(file="/home/jensluch/www/inst_err_log.csv"))
        error.log <- rbind(error.log, c (conditionMessage(e),qry,nchar(qry)))
        write.csv(error.log, file="/home/jensluch/www/inst_err_log.csv", row.names=FALSE)
    })
}
#write.csv(feed.df, file = "Dropbox/BitBank/new/R/instagram/inst_feed.csv", na = 'NA')

#feed comments&likes

feed_comments<-NULL
n_com<-0
for(i in 1:N){n_com<-n_com+length(jdata$data[[i]]$comments$data)}
feed_comments<-matrix(0,n_com,5)
likes<-NULL
likes<-matrix(0,likes_cnt,6)
#counter vars
c<-1
m<-1
N<-length(jdata$data)
#godlike  
for(i in 1:N){
    k<-length(jdata$data[[i]]$comments$data)
    if(jdata$data[[i]]$likes$count!=0){likes_dat<-inst(method=methods[3],id=jdata$data[[i]]$id,option=options[5],token=token)
                                       l<-length(likes_dat$data)
                                       Sys.sleep(0.2)
                                       if(l!=0){
                                           for(h in 1:l){
                                               likes[m,1]<-format(paste(jdata$data[[i]]$id))
                                               likes[m,2]<-format(paste(likes_dat$data[[h]]$bio))
                                               likes[m,3]<-format(paste(likes_dat$data[[h]]$website))
                                               likes[m,4]<-format(paste(likes_dat$data[[h]]$full_name))
                                               likes[m,5]<-format(paste(likes_dat$data[[h]]$id))
                                               likes[m,6]<-format(likes_dat$data[[h]]$username)
                                               m<-m+1
                                           }}
    }
    #if(k==0){next} 
    if(k!=0){
        for(p in 1:k){
            feed_comments[c,1]<-format(jdata$data[[i]]$comments$data[[p]]$id)
            feed_comments[c,2]<-format(jdata$data[[i]]$comments$data[[p]]$text[1])
            feed_comments[c,3]<-format(jdata$data[[i]]$comments$data[[p]]$from$username)
            feed_comments[c,4]<-format(jdata$data[[i]]$comments$data[[p]]$from$id)
            feed_comments[c,5]<-format(paste(jdata$data[[i]]$id))
            c<-c+1
            print(c,1)
            print(jdata$data[[i]]$comments$data[[p]]$text)}
    }
    else {print(i)}
}
feed_comments.df<-as.data.frame(feed_comments)
names<-c("Id","text","from_username","from_id", "media_ID")
colnames(feed_comments.df)<-names
a<-paste(userid, feed_comments.df$Id,gsub("'","",feed_comments.df$text),feed_comments.df$from_username,feed_comments.df$from_id,feed_comments.df$media_ID,sep="','")
qry<-sprintf("INSERT INTO sna.inst_feed_comments VALUES('%s');",a)
for(i in 1:length(qry)){
    tryCatch({
        dbSendUpdate(conn=conn,statement=qry[i])
    }, error=function(e){
        #error.log <- NULL
        error.log <- as.matrix(read.csv(file="/home/jensluch/www/inst_err_log.csv"))
        error.log <- rbind(error.log, c (conditionMessage(e),qry,nchar(qry)))
        write.csv(error.log, file="/home/jensluch/www/inst_err_log.csv", row.names=FALSE)
    })
}

#write.csv(feed_comments.df, file = "Dropbox/BitBank/new/R/instagram/feed_comments.csv", na = 'NA')
likes.df<-as.data.frame(likes)
names<-c("media_ID", "bio", "website", "full_name", "userID", "username")
colnames(likes.df)<-names
a<-paste(userid, likes.df$media_ID,likes.df$userID,likes.df$username,sep="','")
qry<-sprintf("INSERT INTO sna.inst_feed_likes VALUES('%s');",a)
for(i in 1:length(qry)){
    tryCatch({
        dbSendUpdate(conn=conn,statement=qry[i])
    }, error=function(e){
        #error.log <- NULL
        error.log <- as.matrix(read.csv(file="/home/jensluch/www/inst_err_log.csv"))
        error.log <- rbind(error.log, c (conditionMessage(e),qry,nchar(qry)))
        write.csv(error.log, file="/home/jensluch/www/inst_err_log.csv", row.names=FALSE)
    })
}


conn.close <- RJDBC::dbDisconnect(conn = conn)
#write.csv(likes.df, file = "Dropbox/BitBank/new/R/instagram/likes.csv", na = 'NA')

#feed

#for internetless purposes
#con=file("C:/Users/pylyp.matyash/Documents/R/json_data.txt",open="r")
#input<-paste(readLines(con),collapse="")
#jdata<-fromJSON(input)


################################## TEST ######################################
#errors <- as.matrix(read.csv(file="/home/jensluch/www/inst_err_log.csv"))
#cat('<!DOCTYPE HTML><html><head><meta charset="utf-8"><title>Inst error counter</title></head><body>')
#cat(sprintf('<p><b>%s %s %s     %s </b></p>', test$user_id, test$first_name, test$last_name, test$quotes))
#cat(sprintf('<p><b> %s %s %s </b></p>', errors[,1], errors[,2], errors[,3] ))
#cat('</body></html>')

#OK
