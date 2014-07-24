rm(list=ls())


Sys.setlocale("LC_ALL", "uk_UA.UTF-8")  
options(RCurlOptions = list(cainfo = system.file("CurlSSL", "cacert.pem", package = "RCurl")))

start_time <- Sys.time()
#library(RJDBC)
library(lib.loc="/usr/lib/R/site-library",package=RJDBC)
#library(RCurl)
library(lib.loc="/usr/lib/R/site-library",package=RCurl)
#library(RJSONIO)
library(lib.loc="/usr/lib/R/site-library",package=RJSONIO)

#
#rJava 500 err fix
#Step 1:
#Add a file to:
#/etc/ld.so.conf.d

#called:
#rApache_rJava.conf

#with just a single line:
#/usr/lib/jvm/java-7-openjdk-amd64/jre/lib/amd64/server/
#which happens to be the direct parent path to libjvm.so on my server.

#Step 2:
#As root, run:
#/sbin/ldconfig

#Step 3:
#Restart Apache
#

####################################################################################################################
#vDriver <- JDBC(driverClass="com.vertica.jdbc.Driver", classPath="/home/jensluch/www/vertica-jdbc-7.0.1-0.jar")   # DB
vDriver <- JDBC(driverClass="com.vertica.jdbc.Driver", classPath="/home/jensluch/www/vertica-jdk5-6.1.3-0.jar")    #
conn <- dbConnect(vDriver, "jdbc:vertica://10.11.5.21:5433/dwhdb", "phil", "Qq12345678")                           # connection
####################################################################################################################

#test_cnt_start <- RJDBC::dbGetQuery(conn, "SELECT count(*) FROM sna.fb_User")

#################################################################################################################### FB_PARAMS
#resp <- SERVER$args
#token <- (unlist((strsplit((unlist(strsplit(resp,'&'))),"="))[1]))[2]
#fb_id <- as.character((RJDBC::dbGetQuery(conn, "SELECT * FROM sna.tokens"))[['user_id']][3])
#token <- as.character((RJDBC::dbGetQuery(conn, "SELECT * FROM sna.tokens"))[['token']][3])
token <- "CAACEdEose0cBAJQEeqbrIC66zhWIfdTM6NxDtV1z3nFXZAxZAJBu3rCrHhEZC2BSmDvX9qVcxUnytg9fMMisAZB5bLIDvFlSvrsZB2CRkSCrLXy4ZA2yKPoV6OqZB01Oajd8GM35bbFKfxzg78FTZAiM3QQPqVhyP2ZAaBtpTZAS1uT8cjyw3ZCDnqXn8fRGcvnFfIZCoks1soRQNM1fZCa3F7sOe16CoeyJb73AZD"
####################################################################################################################

####################################################################################################################
facebook <-  function(path = "me", options="?", access_token){                                                     # FB
    data <- getURL( sprintf( "https://graph.facebook.com/%s%saccess_token=%s", path, options, token ))             # API
    fromJSON( data )                                                                                               # REQUEST
}                                                                                                                  #
####################################################################################################################

#####################################################################################################################################################################
################################################################### SNA.FB_USER #####################################################################################
#####################################################################################################################################################################

user_qry_str <- function(user_data, fb_user_id) {
    #user_data <- as.list(fb_user_friends$data[[2]])
    #fb_user_id <- fb_user$id
    table_values <- sprintf("'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'",
                            user_data['id'], 
                            gsub("'","",user_data['about']), 
                            gsub("'","",user_data['bio']), 
                            user_data['birthday'],
                            if (!is.na(user_data['cover']) & !is.null(user_data[['cover']])) user_data[['cover']][['source']] else "",
                            user_data['email'],
                            gsub("'","",user_data['first_name']),
                            user_data['gender'],
                            if (!is.null(user_data[['hometown']][['id']])) user_data[['hometown']][['id']] else "0",
                            if (!is.null(user_data[['location']][['id']])) user_data[['location']][['id']] else "0",
                            if (!is.null(user_data[['is_verified']])) user_data[['is_verified']] else "F",
                            gsub("'","",user_data['last_name']),
                            user_data['link'],
                            user_data['locale'],
                            gsub("'","",user_data['middle_name']),
                            gsub("'","",user_data['political']),
                            gsub("'","",user_data['quotes']),
                            user_data['relationship_status'],
                            gsub("'","",user_data['religion']),
                            if (!is.null(user_data[['significant_other']][['id']])) user_data[['significant_other']][['id']] else "0",
                            if (!is.null(user_data[['significant_other']][['id']])) user_data[['significant_other']][['name']] else "",
                            if (!is.null(user_data[['verified']])) user_data[['verified']] else "F",
                            user_data['website'],
                            fb_user_id
    )
    table_values <- gsub(table_values, pattern='NULL', replace="")
    table_values
}

########### USER'S Profile ##############
fb_user_names <- "user_id,about,bio,birthday,cover_source,email,first_name,gender,hometown_id,location_id,is_verified,last_name,link,locale,middle_name,political,quotes,relationship_status,religion,significant_id,significant_name,verified,website,is_friend"
profile_fields <- "id,about,bio,birthday,cover,email,first_name,gender,hometown,is_verified,last_name,link,locale,location,middle_name,political,quotes,relationship_status,religion,significant_other,verified,website,devices,favorite_athletes,favorite_teams,inspirational_people,education,languages,work"
fb_user <- facebook(path='me', options=paste('?fields=',profile_fields,'&',sep=''), access_token=token)


RJDBC::dbSendUpdate(conn=conn, sprintf("INSERT INTO sna.fb_User (%s) VALUES (%s);",
                                       fb_user_names,
                                       user_qry_str(user_data=fb_user,
                                                    fb_user_id="0")))

#--------------------------------------------------------- Devices ---------------------------------------------------------------
devices_qry_str <- function(device_data, fb_user_id) {
    table_values <- sprintf("'%s','%s','%s'",
                            fb_user_id,
                            device_data['hardware'], 
                            device_data['os']
                            )
    table_values <- gsub(table_values, pattern='NULL', replace="")
    table_values
}
devices_names <- "user_id,hardware,os"

device_len <- length(fb_user$devices)
for (i in 1:device_len) {
    RJDBC::dbSendUpdate(conn=conn, sprintf("INSERT INTO sna.fb_User_Devices (%s) VALUES (%s);",
                                           devices_names,
                                           devices_qry_str(as.list(fb_user[['devices']][[i]]),
                                                           fb_user[['id']])))
}

#--------------------------------------------------------- Favorite people ---------------------------------------------------------------
favorite_qry_str <- function(athlete_data, fb_user_id) {
    table_values <- sprintf("'%s','%s','%s'",
                            fb_user_id,
                            athlete_data['id'], 
                            gsub("'","",athlete_data['name'])
    )
    table_values <- gsub(table_values, pattern='NULL', replace="")
    table_values
}

#-- Athletes -----------------------------------------------------------
athletes_names <- "user_id,athlete_id,athlete_name"
athletes_len <- length(fb_user$favorite_athletes)
for (i in 1:athletes_len) {
    RJDBC::dbSendUpdate(conn=conn, sprintf("INSERT INTO sna.fb_User_Athletes (%s) VALUES (%s);",
                                           athletes_names,
                                           favorite_qry_str(as.list(fb_user[['favorite_athletes']][[i]]),
                                                            fb_user[['id']])))
}

#-- Teams -----------------------------------------------------------
teams_names <- "user_id,team_id,team_name"
teams_len <- length(fb_user$favorite_teams)
for (i in 1:teams_len) {
    RJDBC::dbSendUpdate(conn=conn, sprintf("INSERT INTO sna.fb_User_Teams (%s) VALUES (%s);",
                                           teams_names,
                                           favorite_qry_str(as.list(fb_user[['favorite_teams']][[i]]),
                                                            fb_user[['id']])))
}

#-- Inspirational -----------------------------------------------------------

inspirational_names <- "user_id,inspirational_id,inspirational_name"
inspirational_len <- length(fb_user$inspirational_people)
for (i in 1:inspirational_len) {
    RJDBC::dbSendUpdate(conn=conn, sprintf("INSERT INTO sna.fb_User_Inspirational (%s) VALUES (%s);",
                                           inspirational_names,
                                           favorite_qry_str(as.list(fb_user[['inspirational_people']][[i]]),
                                                            fb_user[['id']])))
}

#-- Langs -----------------------------------------------------------

langs_names <- "user_id,langs_id,language"
langs_len <- length(fb_user$languages)
for (i in 1:langs_len) {
    RJDBC::dbSendUpdate(conn=conn, sprintf("INSERT INTO sna.fb_User_Langs (%s) VALUES (%s);",
                                           langs_names,
                                           favorite_qry_str(as.list(fb_user[['languages']][[i]]),
                                                            fb_user[['id']])))
}

#--------------------------------------------------------- Education ---------------------------------------------------------------------
education_qry_str <- function(education_data, fb_user_id) {
    table_values <- sprintf("'%s','%s','%s','%s','%s','%s','%s'",
                            fb_user_id,
                            if(!is.null(education_data[['school']][['id']])) as.list(education_data[['school']])['id'] else "0",
                            if(!is.null(education_data[['school']]['name'])) gsub("'","",as.list(education_data[['school']])['name']) else "",
                            if(!is.null(education_data[['concentration']][[1]]['id'])) as.list(education_data[['concentration']][[1]])['id'] else "0", # may be more than 1 conc. will be fixed later
                            if(!is.null(education_data[['concentration']]['name'])) gsub("'","",as.list(education_data[['concentration']])['name']) else "",
                            if(!is.null(education_data[['year']]['name'])) as.list(education_data[['year']])[['name']] else "0",
                            education_data['type']
    )
    table_values <- gsub(table_values, pattern='NULL', replace="")
    table_values
}

fb_education_names <- "user_id,school_id,school_name,concentration_id,concentration_name,accept_year,type"
education_len <- length(fb_user$education)
for (i in 1:education_len) {
    RJDBC::dbSendUpdate(conn=conn, sprintf("INSERT INTO sna.fb_User_Education (%s) VALUES (%s);",
                                           fb_education_names,
                                           education_qry_str(fb_user[['education']][[i]],
                                                             fb_user[['id']])))
}

#--------------------------------------------------------- Work ---------------------------------------------------------------------

work_qry_str <- function(work_data, fb_user_id) {
    table_values <- sprintf("'%s','%s','%s','%s','%s','%s','%s','%s','%s'",
                            work_data[['employer']]['id'],
                            fb_user_id,
                            gsub("'","",work_data['description']), 
                            work_data['end_date'],
                            gsub("'","",work_data[['employer']]['name']),
                            as.list(work_data[['location']])['id'],
                            as.list(work_data[['position']])['id'],
                            as.list(work_data[['position']])['name'],
                            work_data['start_date']
    )
    table_values <- gsub(table_values, pattern='NULL', replace="")
    table_values
}
#-- Projects -----------------------------------------------------------
project_qry_str <- function(project_data, employer_id) {
    table_values <- sprintf("'%s','%s','%s','%s','%s','%s'",
                            project_data['id'],
                            employer_id,
                            gsub("'","",project_data['description']), 
                            project_data['end_date'],
                            gsub("'","",project_data['name']),
                            project_data['start_date']
    )
    table_values <- gsub(table_values, pattern='NULL', replace="")
    table_values
}

fb_colleagues_names <- "project_id,colleague_id,colleague_name"
fb_project_names <- "project_id,employer_id,description,end_date,project_name,start_date"
fb_work_names <- "employer_id,user_id,description,end_date,employer_name,location_id,position_id,position_name,start_date"
work_len <- length(fb_user$work)
for (i in 1:work_len) {
    RJDBC::dbSendUpdate(conn=conn, sprintf("INSERT INTO sna.fb_User_Work (%s) VALUES (%s);",
                                           fb_work_names,
                                           work_qry_str(as.list(fb_user[['work']][[i]]),
                                                        fb_user[['id']])))
    
    #-- Projects -----------------------------------------------------------
    if (length(fb_user$work[[i]]$projects)) {
        project_len <- length(fb_user$work[[i]]$projects)
        for ( j in 1:project_len) {
            RJDBC::dbSendUpdate(conn=conn, sprintf("INSERT INTO sna.fb_User_Projects (%s) VALUES (%s);", 
                                                   fb_project_names, 
                                                   project_qry_str(as.list(fb_user$work[[i]]$projects[[j]]),
                                                                   as.list(fb_user$work[[i]]$employer)[['id']])))
            
            #-- Colleagues -----------------------------------------------------------
            if (length(fb_user$work[[i]]$projects[[j]]$with)) {
                colleagues_len <- length(fb_user$work[[i]]$projects[[j]]$with)
                for ( k in 1:colleagues_len) {
                    RJDBC::dbSendUpdate(conn=conn, sprintf("INSERT INTO sna.fb_User_Colleagues (%s) VALUES (%s);", 
                                                           fb_colleagues_names,
                                                           favorite_qry_str(as.list(fb_user$work[[i]]$projects[[j]]$with[[k]]),
                                                                            fb_user$work[[i]]$projects[[j]]['id'])))
                }
            }
        }
    }
}


########### FRIENDS' Profile ##############
fb_user_friends <- facebook(path="me/friends", options=paste('?fields=',profile_fields,'&',sep=''), access_token=token)

friends_len <- length(fb_user_friends$data)
for ( i in 1:friends_len ) {
    if (is.vector(fb_user_friends$data[[i]])) fb_user_friends$data[[i]] <- as.list(fb_user_friends$data[[i]]) # bug fix
    #print(resp)
    #qry <- qry_str(resp, fb_user.data$id)
    #View(cbind(strsplit(x=qry,split=',')[[1]], strsplit(x=fb_user_names,split=',')[[1]]))
    RJDBC::dbSendUpdate(conn=conn, sprintf("INSERT INTO sna.fb_User (%s) VALUES (%s);",
                                           fb_user_names,
                                           user_qry_str(fb_user_friends$data[[i]],
                                                        fb_user[['id']])))
}



#####################################################################################################################################################################
################################################################### SNA.FB_USER_LIKES ###############################################################################
#####################################################################################################################################################################

fb_like_names <- 'user_id,page_id,about,attire,affiliation,category,cover_source,link,likes,mission,name,phone,username'
likes_fields <- 'id,about,attire,affiliation,category,cover,link,likes,mission,name,phone,username&limit=10000'
fb_user_likes <- facebook(path="me/likes", options=paste('?fields=',likes_fields,'&',sep=''), access_token=token)

likes_qry_str <- function(likes_data, fb_user_id) {
    table_values <- sprintf("'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'",
                            fb_user_id, # user_id
                            likes_data['id'], # page_id
                            gsub("'","",likes_data['about']),
                            likes_data['attire'],
                            likes_data['affiliation'],
                            likes_data['category'],
                            as.list(likes_data[['cover']])['source'],
                            likes_data['link'],
                            likes_data['likes'],
                            gsub("'","",likes_data['mission']),
                            gsub("'","",likes_data['name']),
                            gsub("'","",likes_data['phone']),
                            likes_data['username']
    )
    table_values <- gsub(table_values, pattern='NULL', replace="")
    return(table_values)
}

likes_len <- length(fb_user_likes$data)
for ( i in 1:likes_len ) {
    if (is.vector(fb_user_likes$data[[i]])) fb_user_likes$data[[i]] <- as.list(fb_user_likes$data[[i]]) # bug fix
    RJDBC::dbSendUpdate(conn=conn, sprintf("INSERT INTO sna.fb_User_Likes (%s) VALUES (%s);",
                                           fb_like_names,
                                           likes_qry_str(likes_data=fb_user_likes[['data']][[i]],
                                                         fb_user_id=fb_user['id'])))
}

#####################################################################################################################################################################
################################################################### SNA.FB_USER_GROUPS ##############################################################################
#####################################################################################################################################################################

fb_group_names <- 'user_id,group_id,cover_source,description,email,icon,link,group_name,owner_id,owner_name,privacy,updated_time'
group_fields <- 'id,cover,description,email,icon,link,name,owner,privacy,updated_time&limit=10000'
fb_user_groups <- facebook(path="me/groups", options=paste('?fields=',group_fields,'&',sep=''), access_token=token)
#length((strsplit(split=',',fb_group_names)[[1]]))

groups_qry_str <- function(groups_data, fb_user_id) {
    table_values <- sprintf("'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'",
                            fb_user_id, # user_id
                            groups_data['id'], # group_id
                            as.list(groups_data[['cover']])['source'],
                            gsub("'","",groups_data['description']),
                            groups_data['email'],
                            groups_data['icon'],
                            groups_data['link'],
                            gsub("'","",groups_data['name']),
                            as.list(groups_data[['owner']])['id'],
                            as.list(groups_data[['owner']])['name'],
                            groups_data['privacy'],
                            groups_data['updated_time']
    )
    table_values <- gsub(table_values, pattern='NULL', replace="")
    return(table_values)
}

groups_len <- length(fb_user_groups$data)
for ( i in 1:groups_len ) {
    if (is.vector(fb_user_groups$data[[i]])) fb_user_groups$data[[i]] <- as.list(fb_user_groups$data[[i]]) # bug fix
    RJDBC::dbSendUpdate(conn=conn, sprintf("INSERT INTO sna.fb_User_Groups (%s) VALUES (%s);",
                                           fb_group_names,
                                           groups_qry_str(groups_data=fb_user_groups[['data']][[i]],
                                                          fb_user_id=fb_user['id'])))
}


#####################################################################################################################################################################
################################################################### SNA.FB_USER_POSTS & COMMENTS ####################################################################
#####################################################################################################################################################################

fb_post_names <- 'user_id,post_id,application_name,application_namespace,caption,created_time,description,is_hidden,link,message,link_name,object_id,source,story,updated_time'
fb_comments_names <- 'user_id,post_id,comment_id,created_time,from_id,from_name,message,like_count'

post_fields <- 'id,application,caption,created_time,description,is_hidden,link,message,name,object_id,source,story,updated_time,comments.limit(1000)&limit=1000'
fb_user_posts <- facebook(path="me/posts", options=paste('?fields=',post_fields,'&',sep=''), access_token=token)
#length((strsplit(split=',',fb_post_names)[[1]]))

posts_qry_str <- function(posts_data, fb_user_id) {
    table_values <- sprintf("'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'",
                            fb_user_id, # user_id
                            posts_data['id'], # post_id
                            gsub("'","",as.list(posts_data[['application']])['name']),
                            as.list(posts_data[['application']])['namespace'],
                            gsub("'","",posts_data['caption']),
                            posts_data['created_time'],
                            gsub("'","",posts_data['description']),
                            if (!is.null(posts_data[['is_hidden']])) posts_data[['is_hidden']] else "F",
                            posts_data['link'],
                            gsub("'","",posts_data['message']),
                            gsub("'","",posts_data['name']), # link_name
                            if(!is.null(posts_data[['object_id']])) posts_data['object_id'] else "0",
                            posts_data['source'],
                            gsub("'","",posts_data['story']),
                            posts_data['updated_time']
                            )
    table_values <- gsub(table_values, pattern='NULL', replace="")
    return(table_values)
}

comments_qry_str <- function(comments_data, post_id, fb_user_id) {
    #comments_data <- fb_user_posts$data[[i]]$comments$data[[1]]
    #fb_user_id <- fb_user['id']
    table_values <- sprintf("'%s','%s','%s','%s','%s','%s','%s','%s'",
                            fb_user_id, # user_id
                            post_id,
                            comments_data['id'],
                            comments_data['created_time'],
                            as.list(comments_data[['from']])['id'],
                            gsub("'","",as.list(comments_data[['from']])['name']),
                            gsub("'","",comments_data['message']),
                            comments_data['like_count']
    )
    table_values <- gsub(table_values, pattern='NULL', replace="")
    return(table_values)
}


posts_len <- length(fb_user_posts$data)
#print(posts_len)
for ( a in 1:posts_len ) {
    #if (is.vector(fb_user_posts$data[[i]])) fb_user_posts$data[[i]] <- as.list(fb_user_posts$data[[i]]) # bug fix
    #print(a)
    #print('-post')
    RJDBC::dbSendUpdate(conn=conn, sprintf("INSERT INTO sna.fb_User_Posts (%s) VALUES (%s);",
                                           fb_post_names,
                                           posts_qry_str(posts_data=as.list(fb_user_posts[['data']][[a]]),
                                                         fb_user_id=fb_user['id'])))
    #print('comments' %in% names(fb_user_posts$data[[a]]))
    #tryCatch({
    #-- Comments -----------------------------------------------------------
    if ('comments' %in% names(fb_user_posts$data[[a]])) {
        comment_len <- length(fb_user_posts$data[[a]][['comments']][['data']])
        for ( b in 1:comment_len ) {
            #print(b)
            #print('-comment')
            RJDBC::dbSendUpdate(conn=conn, sprintf("INSERT INTO sna.fb_User_Comments (%s) VALUES (%s);",
                                                   fb_comments_names,
                                                   comments_qry_str(comments_data=fb_user_posts[['data']][[a]][['comments']][['data']][[b]],
                                                                    post_id=fb_user_posts$data[[a]]$id,
                                                                    fb_user_id=fb_user['id'])))
        }
    }
    #},
    #error=function(e){
    #    print(conditionMessage(e))
    #    print(paste(a,'error a'))
    #})
}

    
conn.close <- RJDBC::dbDisconnect(conn = conn)

#OK


# TESTS
############################################### data type change #############################################
#RJDBC::dbSendUpdate(conn=conn, "ALTER TABLE sna.fb_User_Work ALTER COLUMN start_date set data type DATE") #
##############################################################################################################
#RJDBC::dbSendUpdate(conn=conn, "TRUNCATE TABLE sna.fb_User_Comments")
##############################################################################################################
#test_user <- RJDBC::dbGetQuery(conn, "SELECT * FROM sna.fb_User")
#test_likes <- RJDBC::dbGetQuery(conn, "SELECT * FROM sna.fb_User_Likes")
#test_comments <- RJDBC::dbGetQuery(conn, "SELECT * FROM sna.fb_User_Comments")
#for ( i in 1:length(test$user_id) ) {
    ####  fb_User test params  ################################
#    test$user_id <- as.character(test$user_id)               #
#    test$hometown_id <- as.character(test$hometown_id)       #
#    test$location_id <- as.character(test$location_id)       #
#    test$significant_id <- as.character(test$significant_id) #
#    test$is_friend <- as.character(test$is_friend)           #
    ###########################################################
    
    
    ####  fb_User_Likes test params  ##########################
    #test$user_id <- as.character(test$user_id)               #
    #test$page_id <- as.character(test$page_id)               #
    ###########################################################
    
    ####  fb_User_Likes test params  ##########################
    #test$user_id <- as.character(test$user_id)               #
    #test$group_id <- as.character(test$group_id)             #
    #test$owner_id <- as.character(test$owner_id)            #
    ###########################################################
#}
#test_cnt_end <- RJDBC::dbGetQuery(conn, "SELECT count(*) FROM sna.fb_User")
#View(test)

#conn.close <- RJDBC::dbDisconnect(conn = conn)

#cat('<!DOCTYPE HTML><html><head><meta charset="utf-8"><title>FB test</title></head><body>')

#cat( paste('<p><b>',
#           test_user$user_id,
#           test_user$birthday,
#           test_user$first_name,
#           test_user$last_name,
#           "    ",
#           test_user$quotes,
#           '</b></p>',
#           sep = " "))
#
#cat( paste('<p><b>',
#           test_likes$user_id,
#           test_likes$name,
#           test_likes$likes,
#           "    ",
#           test_likes$link,
#           '</b></p>',
#           sep = " "))

#cat( paste('<p><b>',
#           test_comments$user_id,
#           test_comments$comment_id,
#           test_comments$from_name,
#           "    ",
#           test_comments$message,
#           '</b></p>',
#           sep = " "))

#cat(sprintf('<p><b> %s </b></p>',  Sys.time() - start_time))

#cat( paste('<p><b>',
#           "Connection has been closed:",
#           conn.close,
#           '</b></p>'))
#
#cat('</body></html>')

#length(test)

#tryCatch({
#    dbSendUpdate(conn=conn,statement=encodeString(qry))
#}, error=function(e){
#    #error.log <- NULL
#    error.log <- as.matrix(read.csv(file="/home/jensluch/www/vk_err_log.csv"))
#    error.log <- rbind(error.log, c (conditionMessage(e),qry,nchar(qry)))
#    write.csv(error.log, file="/home/jensluch/www/vk_err_log.csv", row.names=FALSE)
#})




