





from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
import streamlit as st


#API key connection
def Api_connect():
    Api_Id="AIzaSyCfpNDDSdLHxUqtPGWy7sCQoB97XSADh0A"

    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name,api_version,developerKey=Api_Id)
    return youtube

youtube=Api_connect()

#get channel information
def get_channel_info(channel_id):
    
    request = youtube.channels().list(
                part = "snippet,contentDetails,Statistics",
                id = channel_id)
            
    response1=request.execute()

    for i in range(0,len(response1["items"])):
        data = dict(
                    Channel_Name = response1["items"][i]["snippet"]["title"],
                    Channel_Id = response1["items"][i]["id"],
                    Subscription_Count= response1["items"][i]["statistics"]["subscriberCount"],
                    Views = response1["items"][i]["statistics"]["viewCount"],
                    Total_Videos = response1["items"][i]["statistics"]["videoCount"],
                    Channel_Description = response1["items"][i]["snippet"]["description"],
                    Playlist_Id = response1["items"][i]["contentDetails"]["relatedPlaylists"]["uploads"],
                    )
        return data
    
    

#get video ids
def get_channel_videos(channel_id):
    video_ids = []
    # get Uploads playlist id
    res = youtube.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        res = youtube.playlistItems().list( 
                                           part = 'snippet',
                                           playlistId = playlist_id, 
                                           maxResults = 50,
                                           pageToken = next_page_token).execute()
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids





#get video information
def get_video_info(video_ids):

    video_data = []

    for video_id in video_ids:
        request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id= video_id)
        response = request.execute()

        for item in response["items"]:
            data = dict(Channel_Name = item['snippet']['channelTitle'],
                        Channel_Id = item['snippet']['channelId'],
                        Video_Id = item['id'],
                        Title = item['snippet']['title'],
                        Tags = item['snippet'].get('tags'),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        Description = item['snippet']['description'],
                        Published_Date = item['snippet']['publishedAt'],
                        Duration = item['contentDetails']['duration'],
                        Views = item['statistics']['viewCount'],
                        Likes = item['statistics'].get('likeCount'),
                        Comments = item['statistics'].get('commentCount'),
                        Favorite_Count = item['statistics']['favoriteCount'],
                        Definition = item['contentDetails']['definition'],
                        Caption_Status = item['contentDetails']['caption']
                        )
            video_data.append(data)
    return video_data


# get comment information
def get_comment_info(video_ids):
    comment_data = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response = request.execute()

            for item in response["items"]:
                data = dict(comment_Id=item["snippet"]["topLevelComment"]["id"],
                            video_Id=item["snippet"]["topLevelComment"]["snippet"]["videoId"],
                            comment_Text=item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                            comment_Author=item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                            comment_Published=item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])

            comment_data.append(data)
    except:
        pass
    return comment_data


#get playlist ids
def get_playlist_info(channel_id):
    All_data = []
    next_page_token = None
    next_page = True
    while next_page:

        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
            )
        response = request.execute()

        for item in response['items']: 
            data={'PlaylistId':item['id'],
                    'Title':item['snippet']['title'],
                    'ChannelId':item['snippet']['channelId'],
                    'ChannelName':item['snippet']['channelTitle'],
                    'PublishedAt':item['snippet']['publishedAt'],
                    'VideoCount':item['contentDetails']['itemCount']}
            All_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            next_page=False
    return All_data
    

# upload to mongodb

client = pymongo.MongoClient("mongodb://localhost:27017")
db = client["youtube_database"]
def channel_details1(channel_id):
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_info(channel_id)
    vi_ids = get_channel_videos(channel_id)
    vi_details = get_video_info(vi_ids)
    com_details = get_comment_info(vi_ids)

    collection1 = db["channel_details1"]
    collection1.insert_one({"channel_information": ch_details, "playlist_information": pl_details,
                            "video_information": vi_details, "comment_information": com_details})
    return "success the data"



def channels_table():
    mydb = {"host": "127.0.0.1",
            "user": "root",
            "database": "youtube_data",
            "port": "3306"}


    connection = mysql.connector.connect(**mydb)
    cursor = connection.cursor()
    print(cursor)


    try:
        create_query = '''CREATE TABLE if not exists channels1(Channel_Name varchar(100),
                                                            Channel_Id varchar(100) primary key,
                                                            Subscribers varchar(100),
                                                            Views varchar(100),
                                                            Total_Videos varchar(100),
                                                            Channel_Description text,
                                                            Playlist_Id varchar(100))'''

        cursor.execute(create_query)
        connection.commit()

    except:
        print("Channels table is already created")


    ch_list = []
    db = client["youtube_database"]
    collection1 = db["channel_details1"]
    for ch_data in collection1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data["channel_information"])

    df = pd.DataFrame(ch_list)


    for index, row in df.iterrows():
        insert_query = '''INSERT INTO channels1(Channel_Name,
                                            Channel_Id,
                                            Subscribers,
                                            Views,
                                            Total_Videos,
                                            Channel_Description,
                                            Playlist_Id)
                                            
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)'''

        values = (row["Channel_Name"],
                row["Channel_Id"],
                row["Subscription_Count"],
                row["Views"],
                row["Total_Videos"],
                row["Channel_Description"],
                row["Playlist_Id"])
        try:
            cursor.execute(insert_query, values)
            connection.commit()

        except:
            print("channels values inserted")


def videos_table():
        mydb = {"host": "127.0.0.1",
                "user": "root",
                "database": "youtube_data",
                "port": "3306"}


        connection = mysql.connector.connect(**mydb)
        cursor = connection.cursor()
        print(cursor)




        try:
                create_query = '''CREATE TABLE if not exists videos1( Channel_Name varchar(150),
                                                                Channel_Id varchar(100),
                                                                Video_Id varchar(50) primary key, 
                                                                Title varchar(150), 
                                                                Tags text,
                                                                Thumbnail varchar(225),
                                                                Description text, 
                                                                Published_Date timestamp,
                                                                Duration interval, 
                                                                Views int, 
                                                                Likes int,
                                                                Comments int,
                                                                Favorite_Count int, 
                                                                Definition varchar(10), 
                                                                Caption_Status varchar(50))''' 
                
        except:
                st.write("videos values already inserted in the table")
                                
        
        vi_list = []
        db = client["youtube_database"]
        collection1 = db["channel_details1"]
        for vi_data in collection1.find({}, {"_id": 0, "video_information": 1}):
         for i in range(len(vi_data["video_information"])):
                vi_list.append(vi_data["video_information"][i])
        df2= pd.DataFrame(vi_list)     



        for index, row in df2.iterrows():
                insert_query = '''  INSERT INTO  videos1 (Channel_Name,
                                                        Channel_Id,
                                                        Video_Id, 
                                                        Title, 
                                                        Tags,
                                                        Thumbnail,
                                                        Description, 
                                                        Published_Date,
                                                        Duration, 
                                                        Views, 
                                                        Likes,
                                                        Comments,
                                                        Favorite_Count, 
                                                        Definition, 
                                                        Caption_Status 
                                                        )
                        
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                        
                values = ( row["Channel_Name"],
                        row["Channel_Id"],
                        row["Video_Id"],
                        row["Title"],
                        row["Tags"],
                        row["Thumbnail"],
                        row["Description"],
                        row["Published_Date"],
                        row["Duration"],
                        row["Views"],
                        row["Likes"],
                        row["Comments"],
                        row["Favorite_Count"],
                        row["Definition"],
                        row["Caption_Status"])
                        


                try:                     
                 cursor.execute(insert_query,values)
                 connection.commit()    
                except:
                 st.write("Channels values are already inserted")



def playlists_table():
        mydb = {"host": "127.0.0.1",
                "user": "root",
                "database": "youtube_data",
                "port": "3306"}


        connection = mysql.connector.connect(**mydb)
        cursor = connection.cursor()
        print(cursor)


       

        try:
                create_query = '''CREATE TABLE if not exists playlists(PlaylistId varchar(100) primary key,
                                                                        Title varchar(80), 
                                                                        ChannelId varchar(100), 
                                                                        ChannelName varchar(100),
                                                                        PublishedAt timestamp,
                                                                        VideoCount int
                                                                        )'''
                cursor.execute(create_query)
                connection.commit()
        except:
                st.write("Playlists Table alredy created") 
                
          
        db = client["youtube_database"]
        collection1 =db["channel_details1"]
        pl_list = []
        for pl_data in collection1.find({},{"_id":0,"playlist_information":1}):
                for i in range(len(pl_data["playlist_information"])):
                        pl_list.append(pl_data["playlist_information"][i])
        df1= pd.DataFrame(pl_list)


        for index,row in df1.iterrows():
                insert_query = '''INSERT into playlists(PlaylistId,
                                                        Title,
                                                        ChannelId,
                                                        ChannelName,
                                                        PublishedAt,
                                                        VideoCount)
                                                VALUES(%s,%s,%s,%s,%s,%s)'''            
                values =(
                        row['PlaylistId'],
                        row['Title'],
                        row['ChannelId'],
                        row['ChannelName'],
                        row['PublishedAt'],
                        row['VideoCount'])
                try:
                                            
                    cursor.execute(insert_query,values)
                    connection.commit()   
                        
                except:
                    st.write("Playlists values are already inserted")       

def comments_table():
    mydb = {"host": "127.0.0.1",
                    "user": "root",
                    "database": "youtube_data",
                    "port": "3306"}


    connection = mysql.connector.connect(**mydb)
    cursor = connection.cursor()
    print(cursor)
    

    try:
        create_query = '''CREATE TABLE if not exists comments(Comment_Id varchar(100) primary key,
                                                              Video_Id varchar(80),
                                                              Comment_Text text, 
                                                              Comment_Author varchar(150),
                                                              Comment_Published timestamp)'''
        cursor.execute(create_query)
        connection.commit()
        
    except:
        st.write("Commentsp Table already created")



    com_list = []
    db = client["youtube_database"]
    collection1 = db["channel_details1"]
    for com_data in collection1.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3= pd.DataFrame(com_list)


    for index, row in df3.iterrows():
                insert_query = '''INSERT INTO comments (Comment_Id,
                                                        Video_Id ,
                                                        Comment_Text,
                                                        Comment_Author,
                                                        Comment_Published)
                                    VALUES (%s, %s, %s, %s, %s) '''

                    
           
                values = ( row['comment_Id'],
                        row['video_Id'],
                        row['comment_Text'],
                        row['comment_Author'],
                        row['comment_Published'])
                
                            
                try:
                    cursor.execute(insert_query,values)
                    connection.commit()
                except:
                    st.write("This comments are already exist in comments table")

def tables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()
    
    return "Tables created success"
    
def show_channels_table():
        ch_list = []
        db = client["youtube_database"]
        collection1 = db["channel_details1"] 
        for ch_data in collection1.find({},{"_id":0,"channel_information":1}):
                ch_list.append(ch_data["channel_information"])
        df = st.dataframe(ch_list)
        
        return df
    
    
def show_playlists_table():
        pl_list = []   
        db = client["youtube_database"]
        collection1 =db["channel_details1"]
        for pl_data in collection1.find({},{"_id":0,"playlist_information":1}):
                for i in range(len(pl_data["playlist_information"])):
                        pl_list.append(pl_data["playlist_information"][i])
        df1= st.dataframe(pl_list)
        
        return df1
    
def show_videos_table():
    vi_list = []
    db = client["youtube_database"]
    collection1 = db["channel_details1"]
    for vi_data in collection1.find({}, {"_id": 0, "video_information": 1}):
         for i in range(len(vi_data["video_information"])):
                vi_list.append(vi_data["video_information"][i])
    df2= st.dataframe(vi_list) 
    
    return df2    

def show_comments_table():
    com_list = []
    db = client["youtube_database"]
    collection1 = db["channel_details1"]
    for com_data in collection1.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3= st.dataframe(com_list)

    return df3

# streamlit part 

with st.sidebar:
    st.title(":black[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("SKILL TAKE AWAY")
    st.caption("Python scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Managment using MongoDB and SQL")
    
    
channel_id = st.text_input("Enter the Channel id")
channels = channel_id.split(',')
channels = [ch.strip() for ch in channels if ch]    


if st.button("Collect and Store data"):
    for channel in channels:
        ch_ids = []
        db = client["Youtube_database"]
        collection1 = db["channel_details1"]
        for ch_data in collection1.find({},{"_id":0,"channel_information":1}):
            ch_ids.append(ch_data["channel_information"]["Channel_Id"])
        if channel in ch_ids:
            st.success("Channel details of the given channel id: " + channel + " already exists")
        else:
            output = channel_details1(channel)
            st.success(output)
            
            
if st.button("Migrate to SQL"):
    display = tables()
    st.success(display)

show_table = st.radio("SELECT THE TABLE FOR VIEW",(":green[channels]",":orange[playlists]",":red[videos]",":blue[comments]"))



if show_table == ":green[channels]":
    show_channels_table()
elif show_table == ":orange[playlists]":
    show_playlists_table()
elif show_table ==":red[videos]":
    show_videos_table()
elif show_table == ":blue[comments]":
    show_comments_table()
    

#SQL connection

mydb = {"host": "127.0.0.1",
        "user": "root",
        "database": "youtube_data",
        "port": "3306"}
connection = mysql.connector.connect(**mydb)
cursor = connection.cursor()
print(cursor)


question = st.selectbox('Please Select Your Question',
                        ('1. All the videos and the Channel Name',
                        '2. Channels with most number of videos',
                        '3. 10 most viewed videos',
                        '4. Comments in each video',
                        '5. Videos with highest likes',
                        '6. likes of all videos',
                        '7. views of each channel',
                        '8. videos published in the year 2022',
                        '9. average duration of all videos in each channel',
                        '10. videos with highest number of comments'))


if question == "1. All the videos and the Channel Name":
        query = '''SELECT Title as videos, Channel_Name as ChannelName FROM videos1'''
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        connection.commit()
        df = pd.DataFrame(results, columns=["Video Title", "Channel Name"])
        st.write(df)


elif question == "2. Channels with most number of videos":
        query = '''SELECT Channel_Name as ChannelName,Total_Videos as NO_Videos FROM channels1 order by Total_Videos desc'''
        cursor.execute(query)
        results1 = cursor.fetchall()
        cursor.close()
        connection.commit()
        df1 = pd.DataFrame(results1, columns=["ChannelName", "NO_Videos"])
        st.write(df1)
        
        

elif question =="3. 10 most viewed videos":
        query3 = '''SELECT Views as views , Channel_Name as ChannelName,Title as VideoTitle from videos1 
                                where Views is not null order by Views desc limit 10;'''
        cursor.execute(query3)
        results2 = cursor.fetchall()
        cursor.close()
        connection.commit() 
        df2=pd.DataFrame(results2, columns = ["views","channel Name","Video Title"]) 
        st.write(df2)  
        
        

elif question == "4. Comments in each video":
        query4 = "SELECT Comments as No_comments ,Title as VideoTitle from videos1 where Comments is not null;"
        cursor.execute(query4)
        results3 = cursor.fetchall()
        cursor.close()
        connection.commit()
        df3= pd.DataFrame(results3, columns=["No Of Comments", "Video Title"])
        st.write(df3)
        
        
elif question == "5. Videos with highest likes":
        query5 = '''select Title as VideoTitle, Channel_Name as ChannelName, Likes as LikesCount from videos1 
                        where Likes is not null order by Likes desc;'''
        cursor.execute(query5)
        results4 = cursor.fetchall()
        cursor.close()
        connection.commit()
        df4 = pd.DataFrame(results4, columns=["video Title","channel Name","like count"])
        st.write(df4)  
        
        
elif question == "6. likes of all videos":
        query6 = '''select Likes as likeCount,Title as VideoTitle from videos1;'''
        cursor.execute(query6)
        results5 = cursor.fetchall()
        cursor.close()
        connection.commit()    
        df5=pd.DataFrame(results5, columns=["like count","video title"])        
        st.write(df5)
                      
                
elif question == "7. views of each channel":
        query7 = "select Channel_Name as ChannelName, Views as Channelviews from channels1;"
        cursor.execute(query7)
        results6=cursor.fetchall()
        cursor.close()
        connection.commit()

        df6=pd.DataFrame(results6, columns=["channel name","total views"])    
        st.write(df6)
        
        
        
elif question == "8. videos published in the year 2022":
        query8 = '''select Title as Video_Title, Published_Date as VideoRelease, Channel_Name as ChannelName from videos1 
                where extract(year from Published_Date) = 2022;'''
        cursor.execute(query8)
        results7=cursor.fetchall()
        cursor.close()
        connection.commit()

        df7=pd.DataFrame(results7,columns=["Name", "Video Publised On", "ChannelName"])  
        st.write(df7)   
        
        
        
       
elif question == "9. average duration of all videos in each channel":
        query9 =  "SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM videos1 GROUP BY Channel_Name;"
        cursor.execute(query9)
        results8 = cursor.fetchall()
        cursor.close()
        connection.commit()

        df8 = pd.DataFrame(results8, columns=['ChannelTitle', 'Average Duration'])


        DF8=[]
        for index, row in df8.iterrows():
                channel_title = row['ChannelTitle']
                average_duration = row['Average Duration']
                average_duration_str = str(average_duration)
                DF8.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
        st.write(pd.DataFrame(DF8))                
                              
                              
elif question == "10. videos with highest number of comments":
        query10 = '''SELECT Title as VideoTitle, Channel_Name as ChannelName, Comments as Comments FROM videos1 
                        where Comments is not null order by Comments desc;'''
        cursor.execute(query10)
        results9 = cursor.fetchall()
        cursor.close()
        connection.commit()

        df9 =pd.DataFrame(results9, columns=['Video Title', 'Channel Name', 'NO Of Comments'])
        st.write(df9)
      
                              
                
                       

                
                
