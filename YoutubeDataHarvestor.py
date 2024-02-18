#Import required libraries
from googleapiclient.discovery import build
from pymongo import MongoClient
import pandas as pd
import mysql.connector as mysql
import streamlit as st

## API parameter definition
def api_param(channel_id): 
    api_key='api_key'
    youtube=build("youtube", "v3", developerKey=api_key)
    return youtube

## Connect to MongoDB
def connect_mongo():
    mongo_connects=MongoClient("mongodb+srv://kars:8OR120O9TR9NKi@cluster0.9g02hih.mongodb.net/?retryWrites=true&w=majority")
    db=mongo_connects['youtube']
    collection=db['channels']
    return collection
        
def fetch_mongo_channels():
    collection=connect_mongo()
    channels=[]
    for item in collection.find({},{'_id':0,'channel_info':1}):
        channels.append([item['channel_info']['channel_id'],item['channel_info']['channel_data']])
    return channels
## Connect to MySQL:
def connect_mysql():
    connection=mysql.connect(
                                host='localhost',
                                user='root',
                                password='12345678',
                                port=3306,
                                database='Youtube'
                                )
    return  connection

## Function - Fetch Channel details
def get_channel_details(youtube,channel_id):
    response = youtube.channels().list(
                                    id=channel_id,
                                    part='id,snippet,statistics,contentDetails,status'
    )
    channel_data=response.execute()

    ## Construct a dictionary for MongoDB
    channel_details={
                'channel_id':channel_data['items'][0]['id'],
                'channel_data':channel_data['items'][0]['snippet']['title'],
                'channel_type':channel_data['items'][0]['kind'],
                'channel_views':channel_data['items'][0][ 'statistics']['viewCount'],
                'channel_descriptions': channel_data['items'][0]['snippet']['description'],
                'channel_status':channel_data['items'][0]['status']['privacyStatus']
                }
    return channel_details

## Function - Fetch Playlist details
def get_playlist_details(youtube,channel_id):
    nextpage=None
    playlist_ids=[]
    playlist_info=[]
    while True:
        playlist_details = youtube.playlists().list(channelId=channel_id,
                                                    part='snippet,contentDetails,id',
                                                    maxResults=50,
                                                    pageToken=nextpage       
            )
        responses = playlist_details.execute()
        for item in responses['items']:
            playlist_ids.append(item['id'])
            info={
                        'playlist_id':item['id'],
                        'channel_id':item['snippet']['channelId'],
                        'playlist_name':item['snippet']['title']
                }
            playlist_info.append(info)
        
        nextpage=responses.get('nextPageToken')
        if nextpage==None:
            break    
    return playlist_info,playlist_ids

## Function - Fetch video_ids and Playlist video relationship
def get_playlist_video_details(youtube,playlist_ids):
    nextpage=None
    vid_ids=[]
    playlist_video_info=[]
    for id in playlist_ids:
        while True:
            request = youtube.playlistItems().list(playlistId=id,
                                                part='snippet,contentDetails',
                                                maxResults=50,
                                                pageToken=nextpage
    
                )
    
            playlist_items= request.execute()
            for item in playlist_items['items']:
                vid_ids.append(item['contentDetails']['videoId'])
                info=dict(
                            playlist_id=item['snippet']['playlistId'],
                            video_id=item['contentDetails']['videoId']
                        )
                playlist_video_info.append(info)
            nextpage=playlist_items.get('nextPageToken') 
            if nextpage==None:
                break
    return playlist_video_info,vid_ids       

## Function - Fetch Video Details
def get_video_details(youtube,vid_ids):
    nextpage=None
    video_info=[]
    for id in vid_ids:
        while True:
            request = youtube.videos().list(id=id,
                                                   part='id,snippet,statistics,contentDetails,player,status',
                                                   maxResults=100,
                                                   pageToken=nextpage

                )

            videos= request.execute()
            for item in videos['items']:
                info=dict(   video_id=item['id'],
                             channel_id=item['snippet']['channelId'],
                             video_name=item['snippet']['title'],
                             video_description=item['snippet']['description'],
                             video_published_date=item['snippet']['publishedAt'],
                             video_thumbnail=item['snippet']['thumbnails']['default']['url'],
                             view_count=item['statistics'].get('viewCount'),
                             like_count=item['statistics'].get('likeCount'),
                             favorite_count=item['statistics'].get('favoriteCount'),
                             comment_count=item['statistics'].get('commentCount'),
                             duration=item['contentDetails']['duration'],
                             status=item['contentDetails']['caption']
                         )
                video_info.append(info)
            nextpage=videos.get('nextPageToken') 
            if nextpage==None:
                break
    

    return video_info

def get_comment_details(youtube,vid_ids):
    nextpage=None
    comment_info=[]
    try:
        for id in vid_ids:
            while True:
                request = youtube.commentThreads().list(videoId=id,
                                                       part='snippet',
                                                       maxResults=100,
                                                       pageToken=nextpage

                    )

                comments= request.execute()
                for item in comments['items']:
                    info={
                            'comment_id':item['snippet']['topLevelComment']['id'],
                            'video_id':item['snippet']['topLevelComment']['snippet']['videoId'],
                            'comment_text':item['snippet']['topLevelComment']['snippet']['textOriginal'],
                            'comment_author':item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            'comment_published_date':item['snippet']['topLevelComment']['snippet']['publishedAt']
                         }
                    comment_info.append(info)
                nextpage=comments.get('nextPageToken') 
                if nextpage==None:
                    break
    except:
        pass  
    return comment_info
    
def create_db():
    try:
        connection=mysql.connect(
                                    host='localhost',
                                    user='root',
                                    password='12345678',
                                    port=3306
                                )
        cursor=connection.cursor()
        query= '''create database Youtube;'''
        cursor.execute(query)
        connection.commit()
    except:
        pass
        
def create_and_insert_channel_table(channel_id):
    connection=mysql.connect(
                            host='localhost',
                            user='root',
                            password='12345678',
                            port=3306,
                            database='Youtube'
                        )
    cursor=connection.cursor()
    try:
        query='''
                CREATE TABLE Channel(
                            channel_id  VARCHAR(255) PRIMARY KEY,           
                            channel_data VARCHAR(255),          
                            channel_type VARCHAR(255),         
                            channel_views INT,        
                            channel_descriptions TEXT,  
                            channelchannel_status  VARCHAR(255) 
                                     );
              '''
        cursor.execute(query)
        connection.commit()
    except:
        pass
    data=[]
    collection=connect_mongo()
    for item in collection.find({},{'channel_info':1}):
        if item['channel_info']['channel_id']==channel_id:
            data.append(item['channel_info'])
        
    channel_df=pd.DataFrame(data)

    query='''INSERT INTO Channel (
                            channel_id,           
                            channel_data,          
                            channel_type,         
                            channel_views,        
                            channel_descriptions,  
                            channelchannel_status 
                        )   VALUES(%s,%s,%s,%s,%s,%s)'''
    values=[]
    for i in range(len(channel_df)):
        values.append((
          channel_df.loc[i]['channel_id']
        ,(channel_df.loc[i]['channel_data'])
        ,(channel_df.loc[i]['channel_type'])
        ,(channel_df.loc[i]['channel_views'])
        ,(channel_df.loc[i]['channel_descriptions'])
        ,(channel_df.loc[i]['channel_status'])))
    cursor.executemany(query,values)
    connection.commit()
    
def create_and_insert_playlist_table(channel_id):
    connection=mysql.connect(
                            host='localhost',
                            user='root',
                            password='12345678',
                            port=3306,
                            database='Youtube'
                        )
    cursor=connection.cursor()
    try:
        query='''
                  CREATE TABLE Playlist(
                                        playlist_id VARCHAR(255) PRIMARY KEY,   
                                        channel_id VARCHAR(255),    
                                        playlistname VARCHAR(255) ,
                                        FOREIGN KEY(channel_id) REFERENCES Channel(channel_id)
                                        );     
              '''
        cursor.execute(query)
        connection.commit()
    except:
        pass
    query=''' 
              INSERT INTO Playlist(
                                    playlist_id ,   
                                    channel_id ,    
                                    playlistname
                                  ) VALUES(%s,%s,%s);     
          '''

    # Playlist dataframe creation
    data=[]
    collection=connect_mongo()
    for item in collection.find({},{'_id':0,'channel_info':1,'playlist_info':1}):
        if item['channel_info']['channel_id']==channel_id:
            for i in range(len(item['playlist_info'])):
                data.append(item['playlist_info'][i])
    playlist_df=pd.DataFrame(data)
    playlist_df=playlist_df.drop_duplicates(subset=['playlist_id'],keep='first')

    values=[]
    for i in range(len(playlist_df)):
        values.append((playlist_df.loc[i]['playlist_id'],
                       playlist_df.loc[i]['channel_id'],
                       playlist_df.loc[i]['playlist_name']))
    cursor.executemany(query,values)
    connection.commit()

def create_and_insert_video_table(channel_id):
    connection=mysql.connect(
                            host='localhost',
                            user='root',
                            password='12345678',
                            port=3306,
                            database='Youtube'
                        )
    cursor=connection.cursor()
    try:

        query='''
                  CREATE TABLE Video(
                                        video_id              VARCHAR(255) PRIMARY KEY,
                                        playlist_id           VARCHAR(255) ,
                                        video_name            VARCHAR(255),
                                        video_description     TEXT,
                                        video_published_date  DATETIME,
                                        video_thumbnail       VARCHAR(255),
                                        view_count            INT,
                                        like_count            INT,
                                        favorite_count        INT,
                                        comment_count         INT,
                                        duration              INT,
                                        caption_status        VARCHAR(255),
                                        FOREIGN KEY(playlist_id)  REFERENCES Playlist(playlist_id)
                                    );  
              '''
        cursor.execute(query)
        connection.commit()
    except:
        pass
    query='''
              INSERT INTO  Video(
                                    video_id             ,
                                    playlist_id          ,
                                    video_name           ,
                                    video_description    ,
                                    video_published_date ,
                                    video_thumbnail      ,
                                    view_count           ,
                                    like_count           ,
                                    favorite_count       ,
                                    comment_count        ,
                                    duration             ,
                                    caption_status       
                                ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
          '''

    # Find playlist_to_video relationship
    data=[]
    collection=connect_mongo()
    for item in collection.find({},{'_id':0,'channel_info':1,'playlist_video_info':1}):
        if item['channel_info']['channel_id']==channel_id:
            for elem in item['playlist_video_info']:
                data.append(elem)
    pl_vid_relation_df=pd.DataFrame(data)

    data=[]
    for item in collection.find({},{'_id':0,'channel_info':1,'video_info':1}):
        if item['channel_info']['channel_id']==channel_id:
            for i in range(len(item['video_info'])):
                data.append(item['video_info'][i])
    video_df=pd.DataFrame(data)
    video_df=video_df.merge(pl_vid_relation_df,how='left',on='video_id')

    # Convert duration to seconds
    i=0
    for x in video_df['duration']:
        if x.find('H')==-1 and x.find('S')==-1:
            dur=(int(x[x.index('T')+1:x.index('M')])*60)
        elif x.find('H')==-1 and x.find('M')==-1:
            dur=(int(x[x.index('T')+1:x.index('S')]))
        elif x.find('H')==-1:
            dur=(int(x[x.index('T')+1:x.index('M')])*60+int(x[x.index('M')+1:x.index('S')]))
        elif x.find('H')!=-1 and x.find('M')!=-1 and x.find('S')!=-1:
            dur=(int(x[x.index('T')+1:x.index('H')])*60*60+int(x[x.index('H')+1:x.index('M')])*60+int(x[x.index('M')+1:x.index('S')]))
        else:
            dur=0
        video_df['duration'][i]=dur
        i+=1
    video_df=video_df.drop_duplicates(subset=['video_id'],keep='first')

    for i in list(video_df.index):
        
        values=(
                         video_df.loc[i]['video_id'],
                         video_df.loc[i]['playlist_id'],
                         video_df.loc[i]['video_name'],
                         video_df.loc[i]['video_description'],
                         video_df.loc[i]['video_published_date'].replace('Z',''),
                         video_df.loc[i]['video_thumbnail'],
                         video_df.loc[i]['view_count'],
                         video_df.loc[i]['like_count'],
                         video_df.loc[i]['favorite_count'],
                         video_df.loc[i]['comment_count'],
                         video_df.loc[i]['duration'],
                         video_df.loc[i]['status'],
                     )
        cursor.execute(query,values)
        connection.commit()

def create_and_insert_comment_table(channel_id):
    connection=mysql.connect(
                            host='localhost',
                            user='root',
                            password='12345678',
                            port=3306,
                            database='Youtube'
                        )
    cursor=connection.cursor()
    try:
        query=  '''
                      CREATE TABLE Comment(
                        comment_id             VARCHAR(255) PRIMARY KEY,
                        video_id               VARCHAR(255),
                        comment_text           TEXT,
                        comment_author         VARCHAR(255),
                        comment_published_date DATETIME,
                        FOREIGN KEY(video_id)  REFERENCES video(video_id)
                        );
                '''
        cursor.execute(query)
        connection.commit()
#       st.write('Comment table created')

    except:
#        st.write('Comment table already exists')
        pass
    query='''
                        insert into Comment(
                         comment_id            
                        ,video_id              
                        ,comment_text          
                        ,comment_author        
                        ,comment_published_date)
                        values (%s,%s,%s,%s,%s);
          '''

    # Comments dataframe creation
    data=[]
    collection=connect_mongo()
    for item in collection.find({},{'_id':0,'channel_info':1,'comment_info':1}):
        if item['channel_info']['channel_id']==channel_id:
            for i in range(len(item['comment_info'])):
                data.append(item['comment_info'][i])
    comments_df=pd.DataFrame(data)
    comments_df=comments_df.drop_duplicates(subset=['comment_id'],keep='first')

    # insertion into comments table
    print(comments_df.info())
    for i in comments_df.index:
        values=(	comments_df.loc[i]['comment_id'],
                    comments_df.loc[i]['video_id'],
                    comments_df.loc[i]['comment_text'],
                    comments_df.loc[i]['comment_author'],
                    comments_df.loc[i]['comment_published_date'].replace('Z',''))
        cursor.execute(query,values)
        connection.commit()    
    
    
## Streamlit app 
st.title(':red[Youtube Data Harvesting]',anchor=False) 
tab1, tab2,tab3 = st.tabs(["1.Extract data to MongoDB ->", "2.Export data to MySQLDB ->","3.Query Data"])

with tab1:  #1.Extract data to MongoDB
    channels=fetch_mongo_channels()
    channel_id=st.text_input("Enter the channel ID here")
    if st.button('Extract data to MongoDB'):
        collection=connect_mongo()
        for item in collection.find({},{'_id':0,'channel_info':1}):
            if item['channel_info']['channel_id']==channel_id:
                exist_in_mongo=True
                break
            else:
                exist_in_mongo=False
        if exist_in_mongo==False:
            youtube=api_param(channel_id)
            channel_info=get_channel_details(youtube,channel_id)
            playlist_info,playlist_ids=get_playlist_details(youtube,channel_id)
            playlist_video_info,vid_ids=get_playlist_video_details(youtube,playlist_ids)
            video_info=get_video_details(youtube,vid_ids)
            comment_info=get_comment_details(youtube,vid_ids)
            # Connect and insert into mongo 
            collection=connect_mongo()
            collection.insert_one({'channel_info':channel_info,'playlist_info':playlist_info,'video_info':video_info,'playlist_video_info':playlist_video_info,'comment_info':comment_info})
            st.success("Data extracted to MongoDB")
        else:
            st.write('Already exists in MongoDB')
    st.subheader('Channels already loaded in MongoDB')
    st.dataframe(channels,column_config={'0':'Channel Id','1':'Channel Name'})

with tab2: #2.Export data to MySQLDB
    channels=fetch_mongo_channels()
    option=st.selectbox('Channel info in MongoDB',channels)
    create_db()
    connection=mysql.connect(
                    host='localhost',
                    user='root',
                    password='12345678',
                    port=3306,
                    database='Youtube'
                )
    cursor=connection.cursor()
    query='''
                SELECT distinct channel_id FROM Channel where channel_id is not null;
          '''
    cursor.execute(query)
    mysql_ids=[]
    for rows in cursor.fetchall():
        mysql_ids.append(rows[0])
    if st.button('Export data to MySQL'): 
        if option[0] in mysql_ids:
            st.write(':orange[Already exists in MySQL DB !!!]')
        else:
            st.write(':green[Data migration in progress]')

            create_and_insert_channel_table(option[0])
            create_and_insert_playlist_table(option[0])
            create_and_insert_video_table(option[0])
            create_and_insert_comment_table(option[0])
            st.success('Data exported to MySQL')
        
with tab3: #3.Query Data
    st.text('Query Data')
    questions=['1. What are the names of all the videos and their corresponding channels?',
               '2. Which channels have the most number of videos, and how many videos do they have?',
               '3. What are the top 10 most viewed videos and their respective channels?',
               '4. How many comments were made on each video, and what are their corresponding video names?',
               '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
               '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
               '7. What is the total number of views for each channel, and what are their corresponding channel names?',
               '8. What are the names of all the channels that have published videos in the year 2022?',
               '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
               '10. Which videos have the highest number of comments, and what are their corresponding channel names?'          
              ]
    question=None
    question=st.selectbox('Choose a question',questions)
    connection=mysql.connect(
                    host='localhost',
                    user='root',
                    password='12345678',
                    port=3306,
                    database='Youtube'
                )
    cursor=connection.cursor()
    if st.button('Query Database'):
        if question=='1. What are the names of all the videos and their corresponding channels?':

            query='''
                    SELECT VIDEO_NAME,CHANNEL_DATA CHANNEL_NAME
                    FROM CHANNEL CH
                    JOIN PLAYLIST PL
                    ON CH.CHANNEL_ID=PL.CHANNEL_ID
                    JOIN VIDEO V
                    ON PL.PLAYLIST_ID=V.PLAYLIST_ID
                    ORDER BY 2,1;
                  '''
            cursor.execute(query)            
            st.write(pd.DataFrame(cursor.fetchall(),columns=['Video Name','Channel Name']))

        elif question=='2. Which channels have the most number of videos, and how many videos do they have?':
            query='''
                      SELECT CHANNEL_DATA CHANNEL_NAME,COUNT(DISTINCT VIDEO_ID)NUMBER_OF_VIDEOS
                      FROM CHANNEL CH
                      JOIN PLAYLIST PL
                      ON CH.CHANNEL_ID=PL.CHANNEL_ID
                      JOIN VIDEO V
                      ON PL.PLAYLIST_ID=V.PLAYLIST_ID
                      GROUP BY 1 order by 2 desc;
                  '''
            cursor.execute(query)            
            st.write(pd.DataFrame(cursor.fetchall(),columns=['Channel Name','Number of videos']))

        elif question=='3. What are the top 10 most viewed videos and their respective channels?':
            query='''
                        SELECT CH.CHANNEL_DATA CHANNEL_NAME,FACT.VIDEO_NAME,NUMBER_OF_VIEWS
                        FROM (
                        SELECT VIDEO_ID,VIDEO_NAME,SUM(VIEW_COUNT)NUMBER_OF_VIEWS
                        FROM VIDEO 
                        GROUP BY 1 ,2
                        ORDER BY 3 DESC LIMIT 10) FACT
                        JOIN VIDEO V
                        ON FACT.VIDEO_ID=V.VIDEO_ID
                        JOIN PLAYLIST PL
                        ON V.PLAYLIST_ID=PL.PLAYLIST_ID
                        JOIN CHANNEL CH
                        ON PL.CHANNEL_ID=CH.CHANNEL_ID;
                  '''
            cursor.execute(query)            
            st.write(pd.DataFrame(cursor.fetchall(),columns=['Channel Name','Video Name','Number of Views']))

        elif question=='4. How many comments were made on each video, and what are their corresponding video names?':
            query='''
                        SELECT VIDEO_NAME,SUM(COMMENT_COUNT)NUMBER_OF_COMMENTS
                        FROM VIDEO V
                        GROUP BY 1 
                        ORDER BY 2 DESC;

                  '''
            cursor.execute(query)            
            st.write(pd.DataFrame(cursor.fetchall(),columns=['Video Name','Number of comments']))

        elif question=='5. Which videos have the highest number of likes, and what are their corresponding channel names?':
            query='''
                        SELECT CHANNEL_DATA CHANNEL_NAME,VIDEO_NAME,SUM(LIKE_COUNT)NUMBER_OF_LIKES
                        FROM VIDEO V
                        JOIN PLAYLIST PL
                        ON V.PLAYLIST_ID=PL.PLAYLIST_ID
                        JOIN CHANNEL CH 
                        ON PL.CHANNEL_ID=CH.CHANNEL_ID
                        GROUP BY 1 ,2
                        ORDER BY 3 DESC;
                  '''
            cursor.execute(query)            
            st.write(pd.DataFrame(cursor.fetchall(),columns=['Channel Name',' Video Name','Number of likes']))

        elif question=='6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
            query='''
                        SELECT VIDEO_NAME,SUM(LIKE_COUNT)NUMBER_OF_LIKES
                        FROM VIDEO V
                        GROUP BY 1 
                        ORDER BY 2 DESC;
                  '''
            cursor.execute(query)            
            st.write(pd.DataFrame(cursor.fetchall(),columns=['Video Name','Number of likes']))
        
        elif question=='7. What is the total number of views for each channel, and what are their corresponding channel names?':
            query='''
                        SELECT CHANNEL_DATA AS CHANNEL_NAME, SUM(CHANNEL_VIEWS)NUMBER_OF_VIEWS
                        FROM CHANNEL
                        GROUP BY 1 order by 2 desc;
                  '''
            cursor.execute(query)            
            st.write(pd.DataFrame(cursor.fetchall(),columns=['Channel Name','Number of Views']))

        elif question=='8. What are the names of all the channels that have published videos in the year 2022?':
            query='''
                        SELECT DISTINCT  CH.CHANNEL_DATA AS CHANNEL_NAME
                        FROM
                        (SELECT   PLAYLIST_ID 
                        FROM VIDEO 
                        WHERE YEAR(video_published_date)=2022
                        )V
                        JOIN PLAYLIST PL
                        ON V.PLAYLIST_ID=PL.PLAYLIST_ID
                        JOIN CHANNEL CH
                        ON PL.CHANNEL_ID=CH.CHANNEL_ID;
                  '''
            cursor.execute(query)            
            st.write(pd.DataFrame(cursor.fetchall(),columns=['Channel Name']))

        elif question=='9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
            query='''
                        SELECT CH.CHANNEL_DATA CHANNEL_NAME,AVG(DURATION) DURATION
                        FROM CHANNEL CH
                        JOIN PLAYLIST PL
                        ON CH.CHANNEL_ID=PL.CHANNEL_ID
                        JOIN VIDEO V
                        ON PL.PLAYLIST_ID=V.PLAYLIST_ID
                        GROUP BY 1 ORDER BY 2 DESC;
                  '''
            cursor.execute(query)            
            st.write(pd.DataFrame(cursor.fetchall(),columns=['Channel Name','Average Duration (Seconds)']))

        elif question=='10. Which videos have the highest number of comments, and what are their corresponding channel names?':
            query='''
                        SELECT CHANNEL_DATA CHANNEL_NAME,VIDEO_NAME,SUM(COMMENT_COUNT)NUMBER_OF_COMMENTS
                        FROM VIDEO V
                        JOIN PLAYLIST PL
                        ON V.PLAYLIST_ID=PL.PLAYLIST_ID
                        JOIN CHANNEL CH 
                        ON PL.CHANNEL_ID=CH.CHANNEL_ID
                        GROUP BY 1 ,2
                        ORDER BY 3 DESC;
                  '''
            cursor.execute(query)            
            st.write(pd.DataFrame(cursor.fetchall(),columns=['Channel Name','Video Name','Number of Comments']))

    
