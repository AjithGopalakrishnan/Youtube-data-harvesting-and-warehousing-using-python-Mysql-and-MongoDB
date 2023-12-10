from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
import streamlit as st


def Api_connect():
    Api_Id="AIzaSyBrYL6ljl8R4Ta8Iz4hCHaJYp1W-ffxDqo"

    api_service_name="youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=Api_Id)

    return youtube

youtube=Api_connect()   

## API CONNECTION

def Api_connect():
    Api_Id="AIzaSyBrYL6ljl8R4Ta8Iz4hCHaJYp1W-ffxDqo"

    api_service_name="youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=Api_Id)

    return youtube

youtube=Api_connect()    

# Get channel information
def get_channel_info(channel_id):
    request=youtube.channels().list(
                    part="snippet,contentDetails,statistics",
                    id=channel_id
    )
    response=request.execute()

    for i in response['items']:
        data=dict(Channel_Name=i["snippet"]["title"],
                Channel_Id=i["id"],
                Subscribers=i['statistics']['subscriberCount'],
                Views=i["statistics"]["viewCount"],
                Total_videos=i["statistics"]["videoCount"],
                Channel_Description=i["snippet"]["description"],
                Playlist_Id=i["contentDetails"]['relatedPlaylists']["uploads"])
    return data 

#To get video ids
def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id='UCj-_RS1guVbze9juOZ5JjFg',
                                        part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
          video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken') 
        if next_page_token is None :
          break
    return video_ids
    

#To get video information
def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response=request.execute()

        for item in response["items"]:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_Date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favorite_Count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_Status=item['contentDetails']['caption']
                    )
            video_data.append(data)
    return video_data        

#To get comment information 
def get_comment_info(video_ids):
    Comment_data=[]
    try:
            for video_id in video_ids:
                request=youtube.commentThreads().list(
                        part="snippet",
                        videoId=video_id,
                        maxResults=50
                )
                response=request.execute()    

                for item in response['items']:
                    data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                            Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                    
                    Comment_data.append(data)
    except:
        pass    
    return Comment_data       

# To get Playlist details
def get_playlist_details(channel_id):
    next_page_token=None
    All_data=[]
    while True:

        request=youtube.playlists().list(
                part='snippet,contentDetails',
                channelId=channel_id,
                maxResults=10,
                pageToken=next_page_token
        )
        response=request.execute()

        for item in response['items']:
            data=dict(Playlist_Id=item['id'],
                    Channel_Id=item['snippet']['channelId'],
                    Title=item['snippet']['title'],
                    Published_Date=item['snippet']['publishedAt'],
                    Channel_Name=item['snippet']['channelTitle'],
                    Video_Count=item['contentDetails']['itemCount'])
            All_data.append(data) 
        
        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break
    return All_data

# Mongodb connection

client=pymongo.MongoClient("mongodb://localhost:27017")
db=client["Youtube_data"]

def channel_details(channel_id):
   ch_details=get_channel_info(channel_id)
   pl_details=get_playlist_details(channel_id)
   vi_ids=get_videos_ids(channel_id)
   vi_details=get_video_info(vi_ids)
   com_details=get_comment_info(vi_ids)
   

   coll1=db["channel_details"]
   coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                     "video_information":vi_details,"comment_information":com_details})
   
   return"Upload completed successfully"
      

def channels_table():
    mysql_connection = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="3127",
        database="youtube_data",
        port="3306"
    )
    cursor = mysql_connection.cursor()

    drop_query = '''drop table if exists channels'''
    cursor.execute(drop_query)
    mysql_connection.commit()

    try:
        create_query = '''create table if not exists channels(
                          Channel_Name varchar(100),
                          Channel_Id varchar(50) primary key,
                          Subscribers bigint,
                          Views bigint,
                          Total_videos int,
                          Channel_Description text,
                          Playlist_Id varchar(80))'''
        cursor.execute(create_query)
        mysql_connection.commit()
    except:
        print("Channel table already created")


ch_list = []
db = client["Youtube_data"]
coll1 = db["channel_details"]
for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
    ch_list.append(ch_data["channel_information"])
df = pd.DataFrame(ch_list)

for index, row in df.iterrows():
        insert_query = '''INSERT INTO channels(Channel_Name,
                                               Channel_Id,
                                               Subscribers,
                                               Views,
                                               Total_videos,
                                               Channel_Description,
                                               Playlist_Id)
                          VALUES(%s, %s, %s, %s, %s, %s, %s)'''
        values = (row['Channel_Name'],
                  row['Channel_Id'],
                  row['Subscribers'],
                  row['Views'],
                  row['Total_videos'],
                  row['Channel_Description'],
                  row['Playlist_Id'])

        try:
            cursor.execute(insert_query, values)
            mysql_connection.commit()
        except:
            print("Channel values are already inserted")

def playlist_table():
    mysql_connection = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="3127",
        database="youtube_data",
        port="3306"
    )
    cursor = mysql_connection.cursor()

    drop_query = '''drop table if exists playlists'''
    cursor.execute(drop_query)
    mysql_connection.commit()

    create_query = '''create table if not exists playlists(
                            Playlist_Id varchar(100) primary key,
                            Title varchar(100),
                            Channel_Id varchar(100),
                            Channel_Name varchar(100),
                            PublishedAt timestamp,
                            Video_Count int
                            )'''
        
            
    cursor.execute(create_query)
    mysql_connection.commit()

    pl_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for pl_data in coll1.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])

    df1 = pd.DataFrame(pl_list)


    for index, row in df1.iterrows():
     insert_query = '''INSERT INTO playlists(Playlist_Id,
                                           Title,
                                           Channel_Id,
                                           Channel_Name,
                                           Video_Count)
                      VALUES(%s, %s, %s, %s, %s)'''
    
    values = (row['Playlist_Id'],
              row['Title'],
              row['Channel_Id'],
              row['Channel_Name'],  
              row['Video_Count'])
    
    cursor.execute(create_query)
    mysql_connection.commit()
 

def videos_table():

    print("Connecting to MySQL...")
    mysql_connection = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="3127",
        database="youtube_data",
        port="3306"
    )

    print("Creating cursor...")
    cursor = mysql_connection.cursor()

    print("Dropping existing table...")
    drop_query = '''drop table if exists videos'''
    cursor.execute(drop_query)
    mysql_connection.commit()

    print("Creating new table...")
    create_query = '''create table if not exists videos (
        Channel_Name Varchar(100),
        Channel_Id varchar(100),
        Video_Id varchar(30),
        Title varchar(150),
        Thumbnail varchar(200),
        Description text,
        Published_Date timestamp,
        Duration varchar(20),
        Views bigint,
        Likes bigint,
        Comments int,
        Favorite_Count int,
        Definition varchar(10),
        Caption_Status varchar(50)
    )'''
    cursor.execute(create_query)
    mysql_connection.commit()

    print("Table creation completed.")

    vi_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])

    df2  = pd.DataFrame(vi_list)


    for index, row in df2.iterrows():
        insert_query = '''INSERT INTO videos(Channel_Name,
                                            Channel_Id,
                                            Video_Id,
                                            Title,
                                            Thumbnail,
                                            Description,
                                            Duration,
                                            Views,
                                            Likes,
                                            Comments,
                                            Favorite_Count,
                                            Definition,
                                            Caption_Status)
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
        
        values = (row['Channel_Name'],
                row['Channel_Id'],
                row['Video_Id'],
                row['Title'],
                row['Thumbnail'],
                row['Description'],
                row['Duration'],
                row['Views'],
                row['Likes'],
                row['Comments'],
                row['Favorite_Count'],
                row['Definition'],
                row['Caption_Status']
                )

        print(values)
        cursor.execute(insert_query, values)
        mysql_connection.commit()



def comments_table():
    mysql_connection = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="3127",
        database="youtube_data",
        port="3306"
    )
    cursor = mysql_connection.cursor()

    drop_query = '''drop table if exists comments'''
    cursor.execute(drop_query)
    mysql_connection.commit()

    create_query = '''create table if not exists comments (
                        Comment_Id varchar(100),
                        Video_Id varchar(50),
                        Comment_Text text,
                        Comment_Author varchar(150)
                        
                    )'''
    
    cursor.execute(create_query)
    mysql_connection.commit()

    com_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])

    df3 = pd.DataFrame(com_list)

    for index, row in df3.iterrows():
        insert_query = '''INSERT INTO Comments(Comment_Id,
                                               Video_Id,
                                               Comment_Text,
                                               Comment_Author
                                               )
                           Values(%s, %s, %s, %s)'''

        values = (row['Comment_Id'],
                  row['Video_Id'],
                  row['Comment_Text'],
                  row['Comment_Author'])

        cursor.execute(insert_query, values)
        mysql_connection.commit()


comments_table()

def tables():
    channels_table()
    playlist_table()
    videos_table()
    comments_table()

    return "Tables Created Successfully"

def show_channels_table():

    ch_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data["channel_information"])
    df = st.dataframe(ch_list)
    return df

def show_playlists_table():
    pl_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for pl_data in coll1.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])

    df1 = st.dataframe(pl_list)

    return df1

def show_videos_table():
    vi_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])

    df2 = st.dataframe(vi_list)

    return df2

def show_comments_table():
    
    com_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])

    df3 = st.dataframe(com_list)
    
    return df3

# Streamlit Code 

with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skill Take Away")
    st.caption("Python Scripting")
    st.caption("Data Scarping")
    st.caption("MongoD")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and SQL")

channel_id=st.text_input("Enter Channel ID Here :")

if st.button("collect and store data"):
    ch_ids=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])

    if channel_id in ch_ids:
        st.success("channel Details of the given channel id already exists")

    else:
        insert=channel_details(channel_id)
        st.success(insert)  
             
if st.button("Migrate to sql"):
    Table=tables()
    st.success(Table)

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channels_table()

elif show_table=="PLAYLISTS":
    show_playlists_table()

elif show_table=="VIDEOS":
    show_videos_table()

elif show_table=="COMMENTS":
    show_comments_table()    
        

mysql_connection = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    password="3127",
    database="youtube_data",
    port="3306"
)
cursor = mysql_connection.cursor()

questions = st.selectbox("Select your question", (
    "1. All the videos and the channel name",
    "2. channels with most of videos",
    "3. Top 10 most viewed videos",
    "4. Comments in each videos",
    "5. Videos with more likes",
    "6. Likes of all videos",
    "7. Views of each channel",
    "8. Video with the highest number of comments"
))

if questions == "1. All the videos and the channel name":
    query1 = '''select title as videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    t1 = cursor.fetchall()
    mysql_connection.commit()
    df = pd.DataFrame(t1, columns=["video title", "channel name"])
    st.write(df)

elif questions == "2. channels with most of videos":
    query2 = '''select channel_name as channelname,total_videos as no_videos from channels
            where total_videos is not null
            order by total_videos desc'''
    cursor.execute(query2)
    t2 = cursor.fetchall()
    mysql_connection.commit()
    df2 = pd.DataFrame(t2, columns=["channel name", "No of videos"])
    st.write(df2)

elif questions == "3. Top 10 most viewed videos":
    query3 = '''select views as views,channel_name as channelname,title as videotitle from videos
               where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    t3 = cursor.fetchall()
    mysql_connection.commit()
    df3 = pd.DataFrame(t3, columns=["views", "channel name", "videotitle"])
    st.write(df3)

elif questions == "4. Comments in each videos":
    query4 = '''select comments as no_comments,title as videotitle from videos where comments is not null'''
    cursor.execute(query4)
    t4 = cursor.fetchall()
    mysql_connection.commit()
    df4 = pd.DataFrame(t4, columns=["no of comments", "videotitle"])
    st.write(df4)

elif questions == "5. Videos with more likes":
    query5 = '''select title as videotitle, channel_name as channelname, likes as likecount
               from videos where likes is not null order by likes desc'''
    cursor.execute(query5)
    t5 = cursor.fetchall()
    mysql_connection.commit()
    df5 = pd.DataFrame(t5, columns=["videotitle", "channelname", "likecount"])
    st.write(df5)

elif questions == "6. Likes of all videos":
    query6 = '''select likes as likecount,title as videotitle from videos'''
    cursor.execute(query6)
    t6 = cursor.fetchall()
    mysql_connection.commit()
    df6 = pd.DataFrame(t6, columns=["likecount", "videotitle"])
    st.write(df6)

elif questions == "7. Views of each channel":
    query7 = '''select channel_name as channelname,views as totalviews from channels
            where views is not null'''
    cursor.execute(query7)
    t7 = cursor.fetchall()
    mysql_connection.commit()
    df7 = pd.DataFrame(t7, columns=["channelname", "totalviews"])
    df7 = pd.DataFrame(t7, columns=["channelname", "totalviews"])
    st.write(df7)

elif questions == "8. Video with highest number of comments":
    query8 = '''select title as videotitle, channel_name as channelname, COALESCE(comments, 0) as comments 
            from videos where comments is not null order by comments desc'''
    cursor.execute(query8)
    t8 = cursor.fetchall()
    mysql_connection.commit()
    df8 = pd.DataFrame(t8, columns=["video title", "channel name", "comments"])
    st.write(df8)

