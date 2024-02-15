**OBJECTIVE**:  

The  Project Objective is to create a Streamlit application that allows users to access and analyze data from multiple YouTube channels. The application should have the following features:

1. Ability to input a YouTube channel ID and retrieve all the relevant data (Channel name, subscribers, total video count, playlist ID, video ID, likes, dislikes, comments of each video) using Google API.  
2. Option to store the data in a MongoDB database as a data lake.  
3. Ability to collect data for up to 10 different YouTube channels and store them in the data lake by clicking a button.  
4. Option to select a channel name and migrate its data from the data lake to a SQL database as tables.  
5. Ability to search and retrieve data from the SQL database using different search options, including joining tables to get channel details.

**Application High level Workflow:**
![image](https://github.com/karthik-guruparan/YoutubeDataHarvestor/assets/77478705/98d85b36-eb0a-4e32-968e-ef44e63be329)

**Application Walkthrough**
1. When is application is started the user sees the following screen where they can enter the channel ID for which they want to extract the  info into MongoDB. We can also see a table below which displays all the channels which were already loaded into the MongoDB.
   
![image](https://github.com/karthik-guruparan/YoutubeDataHarvestor/assets/77478705/46a5d939-c96e-485a-8e5e-a5ecb804e0b3)

2. If user enters a channel ID which already exists in MongoDB, it will display a message as follows.

 ![image](https://github.com/karthik-guruparan/YoutubeDataHarvestor/assets/77478705/1900bab7-0f13-44c6-a60b-effda6ac41ce)
 
3. Once user enters a new channel ID and clicks on the button, data is extracted into MongoDB.

![image](https://github.com/karthik-guruparan/YoutubeDataHarvestor/assets/77478705/1eb247ad-d76a-45c2-adaf-40cf5b05e5df)

4. Next the user can click on "2.Export data to MySQLDB" and choose a channel which is present in MongoDB and click on the export button. If data exists already in MySQL DB it will not insert.

![image](https://github.com/karthik-guruparan/YoutubeDataHarvestor/assets/77478705/da3e4aac-5a0f-4da5-b733-67b59f057615)

5. When user chooses a new channel and hits export it inserts data in MySQL DB.

![image](https://github.com/karthik-guruparan/YoutubeDataHarvestor/assets/77478705/da78035e-b614-4bb0-8901-650a3fb3db6b)

6. The user needs to repeat previous steps by entering different youtube channel ID so that we have good amount of data to analyse in the final step.
7. As a final step user can now click on "3.Query Data" and choose any of the query and click on the button. The user gets an output in the tabular format.
   
![image](https://github.com/karthik-guruparan/YoutubeDataHarvestor/assets/77478705/174bc33e-b765-47dd-b64d-900d39606e7a)



