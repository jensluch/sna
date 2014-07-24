rm(list=ls())
systime<-Sys.time()

require(RCurl)
require(rjson)
require(RJDBC)


drv <- JDBC("com.vertica.jdbc.Driver", "/opt/vertica/java/lib//vertica-jdbc-7.0.1-0.jar")
conn <- dbConnect(drv, "jdbc:vertica://10.11.5.21:5433/dwhdb", "phil", "Qq12345678")

#oauth.vk.com/authorize?client_id=4463203&redirect_uri=https://oauth.vk.com/blank.html&response_type=token&display=popup&scope=friends%2Cphotos%2Cdocs%2Cnotes%2Cgroups%2Coffline&v=5.23

###vk_profile
#data<-fromJSON(getURL("https://api.vk.com/method/users.get?extended=1&access_token=8b81eb9c054181d820048368425b1949fbeddcac3962032bfa23fab4dc9026afba94b095a46b8dcec7bb7"))
#access_token= "8b81eb9c054181d820048368425b1949fbeddcac3962032bfa23fab4dc9026afba94b095a46b8dcec7bb7"#"5ee07b3838e37999a7f9637e75ee40b93c3bb40c563565656df41ee7422cdeec6e6e21ae987a7c61b4dbb" 5e0748bc87297990163a9a82318206bdd027c175dea5acbe04ee1b4cc7e27e97e64e86a2024e15acfc198
#declare fields and version
fields="fields=sex,bdate,city,country,photo_max_orig,lists,domain,has_mobile,contacts,connections,site,personal,lists,universities,schools,status,last_seen,relation,relatives,counters,timezone,occupation,interests,activities,music,movies,tv,books,games,about,groups,qoutes"
#method=c("users.get", "friends.get", "groups.get")
#userid<-"6467993" #"10535238"
  #"3531787" #257967151
vk <-  function(userid="", method="", resp_format="", fields="", access_token=""){
  if("xml"==resp_format)  { method<-paste(method,".xml",sep="") }
  
  
  #options=paste("uid=",user_id,"&fields=uid,first_name,last_name,nickname,sex,bdate,city,country,has_mobile,contacts,education",sep="")
  #access_token=AccessToken
  
  json_data <- getURL( sprintf( "https://api.vk.com/method/%s?user_id=%s&%s&v=5.21&access_token=%s", method, userid, fields,access_token ))
  cat(json_data, file = "/home/phil/Documents/loggg.json")
  data <- fromJSON(gsub("'","",json_data),unexpected.escape="keep")
}


###define function to grab profile
profile<-function(userid=""){  
data<-vk(userid,method="users.get",fields=fields,access_token=access_token)
if (data$response[[1]]$first_name!="DELETED"){
if(length(data$response[[1]]$personal$political)==0){data$response[[1]]$personal$political<-0}
if(length(data$response[[1]]$personal$people_main)==0){data$response[[1]]$personal$people_main<-0}
if(length(data$response[[1]]$personal$life_main)==0){data$response[[1]]$personal$life_main<-0}
if(length(data$response[[1]]$personal$smoking)==0){data$response[[1]]$personal$smoking<-0}
if(length(data$response[[1]]$personal$alcohol)==0){data$response[[1]]$personal$alcohol<-0}
if(length(data$response[[1]]$occupation$id)==0){data$response[[1]]$occupation$id<-0}
if(length(data$response[[1]]$facebook)==0){data$response[[1]]$facebook<-0}
if(length(data$response[[1]]$counters$groups)==0){data$response[[1]]$counters$groups<-0}
if(length(data$response[[1]]$counters$albums)==0){data$response[[1]]$counters$albums<-0}
if(length(data$response[[1]]$counters$videos)==0){data$response[[1]]$counters$videos<-0}
if(length(data$response[[1]]$counters$audios)==0){data$response[[1]]$counters$audios<-0}
if(length(data$response[[1]]$counters$photos)==0){data$response[[1]]$counters$photos<-0}
if(length(data$response[[1]]$counters$notes)==0){data$response[[1]]$counters$notes<-0}
if(length(data$response[[1]]$counters$friends)==0){data$response[[1]]$counters$friends<-0}
if(length(data$response[[1]]$relation)==0){data$response[[1]]$relation<-0}
lang<-NULL
for (i in length(data$response[[1]]$personal$langs)){lang<-sprintf("%s, %s", lang, data$response[[1]]$personal$langs[[i]])}
# ? data$response[[1]]$mobile_phone
a<-paste(data$response[[1]]$id,data$response[[1]]$first_name,data$response[[1]]$last_name,data$response[[1]]$sex,data$response[[1]]$bdate,data$response[[1]]$city$title,
         data$response[[1]]$country$title,data$response[[1]]$home_town,data$response[[1]]$photo_max_orig,data$response[[1]]$domain,data$response[[1]]$mobile_phone,
         data$response[[1]]$home_phone,data$response[[1]]$site,data$response[[1]]$status,data$response[[1]]$counters$albums,data$response[[1]]$counters$videos,
         data$response[[1]]$counters$audios,data$response[[1]]$counters$photos,data$response[[1]]$counters$notes,data$response[[1]]$counters$friends,data$response[[1]]$counters$groups,
         data$response[[1]]$occupation$type,data$response[[1]]$occupation$id,data$response[[1]]$occupation$name,data$response[[1]]$relation,data$response[[1]]$nickname,
         data$response[[1]]$facebook,data$response[[1]]$facebook_name,data$response[[1]]$instagram,data$response[[1]]$personal$political,lang,
         data$response[[1]]$personal$religion,data$response[[1]]$personal$inspired_by,data$response[[1]]$personal$people_main,data$response[[1]]$personal$life_main,
         data$response[[1]]$personal$smoking,data$response[[1]]$personal$alcohol,data$response[[1]]$activities,data$response[[1]]$interests,data$response[[1]]$music,
         data$response[[1]]$movies,data$response[[1]]$tv,data$response[[1]]$books,data$response[[1]]$games,data$response[[1]]$about,data$response[[1]]$qoutes,sep="','")
qry<-sprintf("INSERT INTO sna.vk_User VALUES('%s');",a)
print(nchar(qry))
print(qry)
dbSendUpdate(conn=conn,statement=encodeString(qry))

}
}


###get user id
user<-fromJSON(getURL(sprintf("https://api.vk.com/method/users.get?v=5.21&access_token=%s",access_token)))
#vk user profile
profile(user$response[[1]]$id)
userid=user$response[[1]]$id
###vk friends
friends<-vk(userid=user$response[[1]]$id,method="friends.get",access_token=access_token)
if(length(friends$response$items)!=0){
for(f in 1:length(friends$response$items)){profile(friends$response$items[f]) 
                                           print(f)}
}

#groups.get
grp<-vk(userid=userid,method="groups.get",fields="extended=1", access_token=access_token)

#insert groups into database 
if(grp$response$count!=0){
for(i in 1:grp$response$count){
b<-paste(userid, grp$response$items[[i]]$id,grp$response$items[[i]]$name,grp$response$items[[i]]$screen_name,grp$response$items[[i]]$type,grp$response$items[[i]]$is_admin,grp$response$items[[i]]$photo_200,sep="','")
qry<-sprintf("INSERT INTO sna.vk_user_groups VALUES('%s');",b)
print(qry)
dbSendUpdate(conn=conn,statement=encodeString(qry))
}
}
#wall.get
wall<-vk(userid=userid,method="wall.get",fields="extended=1&count=100", access_token=access_token)

####insert wall posts into database and get ids of posts which have geo coordinates
if(wall$response$count!=0){
for (i in 1:length(wall$response$items)){
  b<-paste(wall$response$items[[i]]$id, userid,wall$response$items[[i]]$from_id,wall$response$items[[i]]$date,gsub("'","",wall$response$items[[i]]$text),wall$response$items[[i]]$reposts$count,wall$response$items[[i]]$likes$count,wall$response$items[[i]]$post_source$platform,sep="','")
  qry<-sprintf("INSERT INTO sna.vk_user_wall VALUES('%s');",b)
  print(qry)
  dbSendUpdate(conn=conn,statement=encodeString(qry))
}
}
#places.getCheckins
chk<-vk(userid=userid,method="places.getCheckins",fields="extended=1", access_token=access_token)
#get check-in list
if(chk$response$count!=0){
  posts<-paste(sapply(chk$response$items, function(x) x$id),sep='"',collapse=',')
  #get all checkin info
  chkn<-vk(userid=userid,method="wall.getById",fields=sprintf("posts=%s",posts),access_token=access_token)

  for(k in 1:length(chkn$response)){
    cors<-strsplit(chkn$response[[k]]$geo$coordinates,split=" ")
    chkn$response[[k]]$geo$place$latitude<-cors[[1]][1]
    chkn$response[[k]]$geo$place$longitude<-cors[[1]][2]

    b<-paste(chkn$response[[k]]$id,chkn$response[[k]]$geo$place$latitude,chkn$response[[k]]$geo$place$longitude,chkn$response[[k]]$geo$type,gsub("'","",x=chkn$response[[k]]$geo$place$title),chkn$response[[k]]$geo$place$country,gsub("'","",x=chkn$response[[k]]$geo$place$city),sep="','")
    qry<-sprintf("INSERT INTO sna.vk_user_checkins VALUES('%s');",b)
    print(qry)
    dbSendUpdate(conn=conn,statement=encodeString(qry))
}
}
a<-NULL
a$count<-sapply(wall$response$items, function(x) x$comments$count)
a$id<-sapply(wall$response$items, function(x) x$id)
post_id<-a$id[a$count!=0]
com<-NULL
#post_id=NULL

#wall.getComments
if(length(post_id)!=0){ 
  for(i in 1:length(post_id)){
  com[i]<-vk(userid=userid, method="wall.getComments",fields=paste(sprintf('post_id=%s',post_id[i]),"&need_likes=1",sep=""),access_token=access_token)

    for(k in 1:length(com[[i]]$items)){if(length(com[[i]]$items)!=0){
      c<-paste(post_id[i], com[[i]]$items[[k]]$from_id,com[[i]]$items[[k]]$date,gsub("'","",com[[i]]$items[[k]]$text),com[[i]]$items[[k]]$likes$count,com[[i]]$items[[k]]$likes$user_likes,sep="','")
      qry<-sprintf("INSERT INTO sna.vk_user_wall_comments VALUES('%s');",c)
      print(qry)
      dbSendUpdate(conn=conn,statement=encodeString(qry))
      }
    }
  }
}
####faves.get
fave_posts<-vk(userid=userid,method="fave.getPosts",access_token=access_token)
if(length(fave_posts$response$items)!=0){
for(k in 1:length(fave_posts$response$items)){
  if(length(fave_posts$response$items[[k]]$attachments[[1]]$photo$id)==0){fave_posts$response$items[[k]]$attachments[[1]]$photo$id<-0 
                                                                          fave_posts$response$items[[k]]$attachments[[1]]$photo$owner_id<-0
                                                                          fave_posts$response$items[[k]]$attachments[[1]]$photo$photo_604<-0}
  c<-paste(fave_posts$response$items[[k]]$id, fave_posts$response$items[[k]]$from_id, fave_posts$response$items[[k]]$owner_id, fave_posts$response$items[[k]]$date, fave_posts$response$items[[k]]$post_type, 
           gsub("'","",fave_posts$response$items[[k]]$text), fave_posts$response$items[[k]]$attachments[[1]]$photo$id, fave_posts$response$items[[k]]$attachments[[1]]$photo$owner_id, 
           fave_posts$response$items[[k]]$attachments[[1]]$photo$photo_604,fave_posts$response$items[[k]]$likes$count,sep="','")
  qry<-sprintf("INSERT INTO sna.vk_user_fave_posts VALUES('%s');",c)
  print(qry)
  dbSendUpdate(conn=conn,statement=encodeString(qry))
}
}  
#fave_links<-vk(userid=userid,method="fave.getLinks",access_token=access_token)
fave_photos<-vk(userid=userid,method="fave.getPhotos",access_token=access_token)
if(length(fave_photos$response$items)!=0){
for(k in 1:length(fave_photos$response$items)){
  if(length(fave_photos$response$items[[k]]$post_id)==0){fave_photos$response$items[[k]]$post_id<-0}
  c<-paste(fave_photos$response$items[[k]]$id, fave_photos$response$items[[k]]$owner_id, fave_photos$response$items[[k]]$photo_604,
  fave_photos$response$items[[k]]$date, gsub("'","",fave_photos$response$items[[k]]$text),fave_photos$response$items[[1]]$post_id,sep="','") 
  qry<-sprintf("INSERT INTO sna.vk_user_fave_photos VALUES('%s');",c)
  print(qry)
  dbSendUpdate(conn=conn,statement=encodeString(qry))
}  
}
         
  
fave_videos<-vk(userid=userid,method="fave.getVideos",access_token=access_token)
#fave_users<-vk(userid=userid,method="fave.getUsers",access_token=access_token)
if(length(fave_videos$response$items)!=0){
for(k in 1:length(fave_videos$response$items)){
  c<-paste(fave_videos$response$items[[k]]$id,fave_videos$response$items[[k]]$owner_id,gsub("'","",fave_videos$response$items[[k]]$title),
  gsub("'","",fave_videos$response$items[[k]]$description),fave_videos$response$items[[k]]$date,fave_videos$response$items[[k]]$views,fave_videos$response$items[[k]]$comments,sep="','")
  qry<-sprintf("INSERT INTO sna.vk_user_fave_videos VALUES('%s');",c)
  print(qry)
  dbSendUpdate(conn=conn,statement=encodeString(qry))
}  
}
#wall.attachments (for considereation)
#att<-sapply(wall$response$items, function(x) x$attachments)
#att[att!="NULL"]

#####debug
#b<-sapply(fave_photos$response$items, function(x) x$id)
#sapply(chk$response$items, function(x) x$post_id)
#sapply(wall$response$items, function(x) x$attachments)
#a<-sapply(fave_posts$response$items, function(x) x$attachments[[1]]$photo$id)
#table<-dbGetQuery(conn=conn,statement="select * from sna.vk_user_fave_videos;")
#View(table)
dbDisconnect(conn)
print(Sys.time() - systime)
