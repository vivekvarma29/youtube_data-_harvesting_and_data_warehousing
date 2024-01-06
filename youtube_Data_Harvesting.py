from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
import streamlit as st


# API key connection
def Api_connect():
    Api_Id = "AIzaSyDLs5a-sARvyB7Qw-vZ9c-bEPKqft1FTh4"
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=Api_Id)
    return youtube

youtube = Api_connect()


# get channel information
def get_channel_info(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,Statistics",
        id=channel_id)

    response1 = request.execute()

    for i in range(0, len(response1["items"])):
        data = dict(
            Channel_Name=response1["items"][i]["snippet"]["title"],
            Channel_Id=response1["items"][i]["id"],
            Subscription_Count=response1["items"][i]["statistics"]["subscriberCount"],
            Views=response1["items"][i]["statistics"]["viewCount"],
            Total_Videos=response1["items"][i]["statistics"]["videoCount"],
            Channel_Description=response1["items"][i]["snippet"]["description"],
            Playlist_Id=response1["items"][i]["contentDetails"]["relatedPlaylists"]["uploads"],
        )
        return data


# get playlist ids
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
            data = {'PlaylistId': item['id'],
                    'Title': item['snippet']['title'],
                    'ChannelId': item['snippet']['channelId'],
                    'ChannelName': item['snippet']['channelTitle'],
                    'PublishedAt': item['snippet']['publishedAt'],
                    'VideoCount': item['contentDetails']['itemCount']}
            All_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            next_page = False
    return All_data


# get video ids
def get_channel_videos(channel_id):
    video_ids = []
    # get Uploads playlist id
    res = youtube.channels().list(id=channel_id,
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None

    while True:
        res = youtube.playlistItems().list(
            part='snippet',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token).execute()

        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids


# get video information
def get_video_info(video_ids):
    video_data = []

    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id)
        response = request.execute()

        for item in response["items"]:
            data = dict(Channel_Name=item['snippet']['channelTitle'],
                        Channel_Id=item['snippet']['channelId'],
                        Video_Id=item['id'],
                        Title=item['snippet']['title'],
                        Tags=item['snippet'].get('tags'),
                        Thumbnail=item['snippet']['thumbnails']['default']['url'],
                        Description=item['snippet']['description'],
                        Published_Date=item['snippet']['publishedAt'],
                        Duration=item['contentDetails']['duration'],
                        Views=item['statistics']['viewCount'],
                        Likes=item['statistics'].get('likeCount'),
                        Dislikes=item['statistics'].get('dislikeCount'),
                        Comments=item['statistics'].get('commentCount'),
                        Favorite_Count=item['statistics']['favoriteCount'],
                        Definition=item['contentDetails']['definition'],
                        Caption_Status=item['contentDetails']['caption']
                        )
            video_data.append(data)
    return video_data


# get comment information
def get_comment_info(video_ids):
    Comment_Information = []
    try:
        for video_id in video_ids:

            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response5 = request.execute()

            for item in response5["items"]:
                comment_information = dict(
                    Comment_Id=item["snippet"]["topLevelComment"]["id"],
                    Video_Id=item["snippet"]["videoId"],
                    Comment_Text=item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                    Comment_Author=item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                    Comment_Published=item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])

                Comment_Information.append(comment_information)
    except:
        pass

    return Comment_Information


# MongoDB Connection
client = pymongo.MongoClient("mongodb://localhost:27017")
db = client["Youtube_data"]


# upload to MongoDB
def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_info(channel_id)
    vi_ids = get_channel_videos(channel_id)
    vi_details = get_video_info(vi_ids)
    com_details = get_comment_info(vi_ids)

    coll1 = db["channel_details"]
    coll1.insert_one(
        {"channel_information": ch_details, "playlist_information": pl_details, "video_information": vi_details,
         "comment_information": com_details})

    return "upload completed successfully"


# Table creation for channels
def channels_table():
    # SQL connection
    mydb = mysql.connector.connect(host="localhost",
                                   user="root",
                                   password="0000",
                                   database="youtube_database",
                                   port="3306"
                                   )
    cursor = mydb.cursor()

    drop_query = "drop table if exists channels"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''create table if not exists channels(Channel_Name varchar(100),
                        Channel_Id varchar(80) primary key, 
                        Subscription_Count bigint, 
                        Views bigint,
                        Total_Videos int,
                        Channel_Description text,
                        Playlist_Id varchar(50))'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        st.write("Channels Table already created")



# Table creation for playlists
def playlists_table():
    # SQL connection
    mydb = mysql.connector.connect(host="localhost",
                                   user="root",
                                   password="0000",
                                   database="youtube_database",
                                   port="3306"
                                   )
    cursor = mydb.cursor()

    drop_query = "drop table if exists playlists"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''create table if not exists playlists(PlaylistId varchar(100) primary key,
                        Title varchar(80), 
                        ChannelId varchar(100), 
                        ChannelName varchar(100),
                        PublishedAt text,
                        VideoCount int
                        )'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        st.write("Playlists Table already created")

# Table creation for videos
def videos_table():
    # SQL connection
    mydb = mysql.connector.connect(host="localhost",
                                   user="root",
                                   password="0000",
                                   database="youtube_database",
                                   port="3306"
                                   )
    cursor = mydb.cursor()

    drop_query = "drop table if exists videos"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''create table if not exists videos(
                        Channel_Name varchar(150),
                        Channel_Id varchar(100),
                        Video_Id varchar(50) primary key, 
                        Title varchar(150), 
                        Tags text,
                        Thumbnail varchar(225),
                        Description text, 
                        Published_Date text,
                        Duration text, 
                        Views bigint, 
                        Likes bigint,
                        Dislikes int,
                        Comments int,
                        Favorite_Count int, 
                        Definition varchar(10), 
                        Caption_Status varchar(50) 
                        )'''

        cursor.execute(create_query)
        mydb.commit()
    except:
        st.write("Videos Table already created")

# Table creation for comments
def comments_table():
    # SQL connection
    mydb = mysql.connector.connect(host="localhost",
                                   user="root",
                                   password="0000",
                                   database="youtube_database",
                                   port="3306"
                                   )
    cursor = mydb.cursor()

    drop_query = "drop table if exists comments"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''CREATE TABLE if not exists comments(Comment_Id varchar(100) primary key,
                       Video_Id varchar(80),
                       Comment_Text text, 
                       Comment_Author varchar(150),
                       Comment_Published text)'''
        cursor.execute(create_query)
        mydb.commit()

    except:
        st.write("Comments Table already created")


#All tables calling function
def tables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()
    return "Tables Created successfully"


#shows the channels information
def show_channels_table():
    ch_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data["channel_information"])
    channels_table = st.dataframe(ch_list)
    return channels_table

#shows the playlists information
def show_playlists_table():
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    pl_list = []
    for pl_data in coll1.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    playlists_table = st.dataframe(pl_list)
    return playlists_table

#shows the videos information
def show_videos_table():
    vi_list = []
    db = client["Youtube_data"]
    coll2 = db["channel_details"]
    for vi_data in coll2.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    videos_table = st.dataframe(vi_list)
    return videos_table

#shows the comments information
def show_comments_table():
    com_list = []
    db = client["Youtube_data"]
    coll3 = db["channel_details"]
    for com_data in coll3.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    comments_table = st.dataframe(com_list)
    return comments_table

#stream lit text input for channel id
channel_id = st.text_input("Enter the Channel id")
channels = channel_id.split(',')
channels = [ch.strip() for ch in channels if ch]

#stream lit code for UI
def streamlit_code():

    with st.sidebar:
        st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")

        show_table = st.radio("SELECT THE TABLE FOR VIEW",
                              ("----Select----",":green[channels]", ":orange[playlists]", ":red[videos]", ":blue[comments]"))

        if show_table == ":green[channels]":
            show_channels_table()
        elif show_table == ":orange[playlists]":
            show_playlists_table()
        elif show_table == ":red[videos]":
            show_videos_table()
        elif show_table == ":blue[comments]":
            show_comments_table()

#Collects and store data in mongo db

    if st.button("Collect and Store data"):
        for channel in channels:
            ch_ids = []
            db = client["Youtube_data"]
            coll1 = db["channel_details"]
            for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
                ch_ids.append(ch_data["channel_information"]["Channel_Id"])
            if channel in ch_ids:
                st.success("Channel details of the given channel id: " + channel + " already exists")
            else:
                output = channel_details(channel)
                st.success(output)

#It shows store channels details
    if st.button("view stored channels"):
        display_channel_id_name = []
        db = client["Youtube_data"]
        coll1 = db["channel_details"]
        for chan_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
            d={'Channel Name':chan_data["channel_information"]["Channel_Name"],
               'Channel Id':chan_data["channel_information"]["Channel_Id"],
               'Subscription_Count':chan_data["channel_information"]["Subscription_Count"],
               'Views':chan_data["channel_information"]["Views"],
               'Total Videos':chan_data["channel_information"]["Total_Videos"]}

            display_channel_id_name.append(d)

        channels_details = st.dataframe(display_channel_id_name)
        return channels_details

streamlit_code()


def migrate_sql():
    if st.button("Migrate to SQL"):
        # mysql connection
        mydb = mysql.connector.connect(host="localhost",
                                       user="root",
                                       password="0000",
                                       database="youtube_database",
                                       port="3306"
                                       )
        cursor = mydb.cursor()
        # Assuming you have a specific channel ID you want to insert, let's call it 'selected_channel_id'
        selected_channel_id = channels

        db = client["Youtube_data"]
        coll1 = db["channel_details"]

    # Iterate over MongoDB channels and insert data into MySQL only for the selected channel ID
        for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
            if ch_data["channel_information"]["Channel_Id"] in selected_channel_id:
                ch_details = ch_data["channel_information"]
                pl_details = get_playlist_info(ch_details["Channel_Id"])
                vi_ids = get_channel_videos(ch_details["Channel_Id"])
                vi_details = get_video_info(vi_ids)
                com_details = get_comment_info(vi_ids)

    # Insert into MySQL channels table
                insert_query_channels = '''
                    INSERT INTO channels(Channel_Name,
                                        Channel_Id,
                                        Subscription_Count,
                                        Views,
                                        Total_Videos,
                                        Channel_Description,
                                        Playlist_Id)
                    VALUES(%s,%s,%s,%s,%s,%s,%s)
                '''
                values_channels = (
                    ch_details['Channel_Name'],
                    ch_details['Channel_Id'],
                    ch_details['Subscription_Count'],
                    ch_details['Views'],
                    ch_details['Total_Videos'],
                    ch_details['Channel_Description'],
                    ch_details['Playlist_Id'])
                try:
                    cursor.execute(insert_query_channels, values_channels)
                    mydb.commit()

                except Exception as e:
                    st.error(f"Error inserting channel data into MySQL: {e}")


    # Insert into MySQL playlists table
                insert_query_playlists = '''
                    INSERT INTO playlists(PlaylistId,
                                          Title,
                                          ChannelId,
                                          ChannelName,
                                          PublishedAt,
                                          VideoCount)
                    VALUES(%s,%s,%s,%s,%s,%s)
                '''
                for pl_data in pl_details:
                    values_playlists = (
                        pl_data['PlaylistId'],
                        pl_data['Title'],
                        pl_data['ChannelId'],
                        pl_data['ChannelName'],
                        pl_data['PublishedAt'],
                        pl_data['VideoCount'])
                    try:
                        cursor.execute(insert_query_playlists, values_playlists)
                        mydb.commit()

                    except Exception as e:
                        st.error(f"Error inserting playlists data into MySQL: {e}")

    # Insert into MySQL videos table
                insert_query_videos = '''
                    INSERT INTO videos(Channel_Name,
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
                                       Dislikes,
                                       Comments,
                                       Favorite_Count,
                                       Definition,
                                       Caption_Status)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                '''
                for vi_data in vi_details:
                    tags_value = str(vi_data['Tags'])
                    values_videos = (
                        vi_data['Channel_Name'],
                        vi_data['Channel_Id'],
                        vi_data['Video_Id'],
                        vi_data['Title'],
                        tags_value,
                        vi_data['Thumbnail'],
                        vi_data['Description'],
                        vi_data['Published_Date'],
                        vi_data['Duration'],
                        vi_data['Views'],
                        vi_data['Likes'],
                        vi_data['Dislikes'],
                        vi_data['Comments'],
                        vi_data['Favorite_Count'],
                        vi_data['Definition'],
                        vi_data['Caption_Status'])
                    try:
                        cursor.execute(insert_query_videos, values_videos)
                        mydb.commit()

                    except Exception as e:
                        st.error(f"Error inserting videos data into MySQL: {e}")

    # Insert into MySQL comments table
                insert_query_comments = '''
                    INSERT INTO comments(Comment_Id,
                                         Video_Id,
                                         Comment_Text,
                                         Comment_Author,
                                         Comment_Published)
                    VALUES(%s,%s,%s,%s,%s)
                '''
                for com_data in com_details:
                    values_comments = (
                        com_data['Comment_Id'],
                        com_data['Video_Id'],
                        com_data['Comment_Text'],
                        com_data['Comment_Author'],
                        com_data['Comment_Published'])
                    try:
                        cursor.execute(insert_query_comments, values_comments)
                        mydb.commit()

                    except Exception as e:
                        st.error(f"Error inserting comments data into MySQL: {e}")

                # Close the MySQL cursor
                cursor.close()

        # Close the MySQL connection
        mydb.close()
        st.success("Migrated Successfully")

migrate_sql()


# SQL connection
mydb = mysql.connector.connect(host="localhost",
                        user="root",
                        password="0000",
                        database="youtube_database",
                        port="3306"
                        )
cursor = mydb.cursor()

question = st.selectbox(
    'Please Select Your Question',
    ('---------Select the questions---------',
     '1.What are the names of all the videos and their corresponding channels?',
     '2.Which channels have the most number of videos, and how many videos do they have?',
     '3.What are the top 10 most viewed videos and their respective channels?',
     '4.How many comments were made on each video, and what are their corresponding video names?',
     '5.Which videos have the highest number of likes, and what are their corresponding channel names?',
     '6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
     '7.What is the total number of views for each channel, and what are their corresponding channel names?',
     '8.What are the names of all the channels that have published videos in the year 2022?',
     '9.What is the average duration of all videos in each channel, and what are their corresponding channel names?',
     '10.Which videos have the highest number of comments, and what are their corresponding channel names?'))


if question=='---------Select the questions---------':
    pass

elif question == '1.What are the names of all the videos and their corresponding channels?':
    query1 = "select Title as videos, Channel_Name as ChannelName from videos;"
    cursor.execute(query1)

    t1 = cursor.fetchall()
    mydb.commit()
    st.write(pd.DataFrame(t1, columns=["Video Title", "Channel Name"]))

elif question == '2.Which channels have the most number of videos, and how many videos do they have?':
    query2 = "select Channel_Name as ChannelName,Total_Videos as NO_Videos from channels order by Total_Videos desc;"
    cursor.execute(query2)
    t2 = cursor.fetchall()
    mydb.commit()
    st.write(pd.DataFrame(t2, columns=["Channel Name", "No Of Videos"]))

elif question == '3.What are the top 10 most viewed videos and their respective channels?':
    query3 = '''select Views as views , Channel_Name as ChannelName,Title as VideoTitle from videos
                        where Views is not null order by Views desc limit 10;'''
    cursor.execute(query3)
    t3 = cursor.fetchall()
    mydb.commit()
    st.write(pd.DataFrame(t3, columns=["views", "channel Name", "video title"]))

elif question == '4.How many comments were made on each video, and what are their corresponding video names?':
    query4 = "select Comments as No_comments ,Title as VideoTitle from videos where Comments is not null;"
    cursor.execute(query4)
    t4 = cursor.fetchall()
    mydb.commit()
    st.write(pd.DataFrame(t4, columns=["No Of Comments", "Video Title"]))

elif question == '5.Which videos have the highest number of likes, and what are their corresponding channel names?':
    query5 = '''select Title as VideoTitle, Channel_Name as ChannelName, Likes as LikesCount from videos
                       where Likes is not null order by Likes desc;'''
    cursor.execute(query5)
    t5 = cursor.fetchall()
    mydb.commit()
    st.write(pd.DataFrame(t5, columns=["video Title", "channel Name", "like count"]))

elif question == '6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
    query6 = '''select Likes as likeCount,Dislikes as DislikeCount,Title as VideoTitle from videos;'''
    cursor.execute(query6)

    t6 = cursor.fetchall()
    mydb.commit()
    st.write(pd.DataFrame(t6, columns=["like count","dislike count","video title"]))

elif question == '7.What is the total number of views for each channel, and what are their corresponding channel names?':
    query7 = "select Channel_Name as ChannelName, Views as Channelviews from channels;"
    cursor.execute(query7)

    t7 = cursor.fetchall()
    mydb.commit()
    st.write(pd.DataFrame(t7, columns=["channel name", "total views"]))

elif question == '8.What are the names of all the channels that have published videos in the year 2022?':
    query8 = '''select Title as Video_Title, Published_Date as VideoRelease, Channel_Name as ChannelName from videos
                where extract(year from Published_Date) = 2022;'''
    cursor.execute(query8)

    t8 = cursor.fetchall()
    mydb.commit()
    st.write(pd.DataFrame(t8, columns=["Name", "Video Publised On", "ChannelName"]))

elif question == '9.What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    query9 = """
    SELECT
        Channel_Name as ChannelTitle,
        SEC_TO_TIME(AVG(TIME_TO_SEC(STR_TO_DATE(Duration, 'PT%iM%sS')))) AS average_duration
    FROM videos
    GROUP BY Channel_Name;
    """
    cursor.execute(query9)
    t9 = cursor.fetchall()
    mydb.commit()
    t9 = pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])

    T9 = []
    for index, row in t9.iterrows():
        channel_title = row['ChannelTitle']
        average_duration = row['Average Duration']
        T9.append({"Channel Title": channel_title, "Average Duration": str(average_duration)})

    # Display the DataFrame
    st.write(pd.DataFrame(T9))


elif question == '10.Which videos have the highest number of comments, and what are their corresponding channel names?':
    query10 = '''select Title as VideoTitle, Channel_Name as ChannelName, Comments as Comments from videos
                       where Comments is not null order by Comments desc;'''
    cursor.execute(query10)

    t10 = cursor.fetchall()
    mydb.commit()
    st.write(pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'NO Of Comments']))
