import sqlite3
import sys
import pandas as pd
import os
import numpy as np
import filetype
import datetime
import shutil
import ntpath
from platform import system
from pathlib import Path
from data import Snapchat_pb2


def protoParse(schema, data, content_type):
    schema.ParseFromString(data)
    if content_type == 0:
        return (schema.Content.startMedia.unknown.unknown.unknown.cacheId,np.nan)
    elif content_type == 1:
        return (schema.Content.chat.chatMessage.message,np.nan)
    elif content_type == 2:
        return (schema.Content.startMedia.unknown.unknown.unknown.cacheId, schema.Content.chat.mediatext.mediatext2.mediatextFinal)
    else: return "Error", np.nan

def encodeChat(message):
    encodedMessage = ''
    try:
        if message != np.nan:
            if len(message) >= 2:
                for i in message:
                    tmp = i.encode('cp1252', 'xmlcharrefreplace')   #Display Emojis
                    tmp = tmp.decode('cp1252')
                    encodedMessage = encodedMessage + tmp
            else:
                i = message[0].encode('cp1252', 'xmlcharrefreplace')
                encodedMessage = i.decode('cp1252')
                
            return encodedMessage
        else: return np.nan
    except Exception as e:
            return """ERROR - Something went wrong when parsing this message. \n Manually verify the message with Client Conversation ID and Server Message ID in arroyo.db"""


def getChats(database):
    print("Parsing messages from: ",database)
    db_arroyo =database
    con_arroyo =sqlite3.connect(db_arroyo)
    query_arroyo ="""select
    client_conversation_id as 'Client Conversation ID',
    server_message_id,
    message_content,
    datetime(creation_timestamp/1000, 'unixepoch') as 'Creation Timestamp',
    datetime(read_timestamp/1000, 'unixepoch') as 'Read Timestamp',
    content_type,
    sender_id as userId
    from conversation_message
    order by client_conversation_id, creation_timestamp
    """
    df_arroyo = pd.read_sql_query(query_arroyo, con_arroyo)
    schema = Snapchat_pb2.root()
    df_arroyo["extramessage_content"] = np.nan
    vect = np.vectorize(protoParse)
    df_arroyo['message_content'], df_arroyo["extramessage_content"] = np.where(df_arroyo['content_type'] == 0, vect(schema, df_arroyo['message_content'], 0),
                                                                               np.where(df_arroyo['content_type']==1,vect(schema, df_arroyo['message_content'], 1),
                                                                                        np.where(df_arroyo['content_type'] == 2,vect(schema, df_arroyo['message_content'], 2), "ERROR - Something went wrong when parsing this message.")))
    df_arroyo['message_content'] = df_arroyo['message_content'].apply(encodeChat)
    df_arroyo['extramessage_content']= df_arroyo['extramessage_content'].apply(encodeChat)
##    for index, row in df_arroyo.iterrows():
##        message, extramessage = (protoParse(schema, row["message_content"], row["content_type"]))
##        df_arroyo.loc[index, row["message_content"]] = encodeChat(message)
##        df_arroyo.loc[index, row["extramessage_content"]] = encodeChat(extramessage)
    #print(df_arroyo['message_content'].head())
    return df_arroyo
def getCore(database):
    con_core = sqlite3.connect(database)
    query_core = """SELECT contentObjectId, cacheKey from DataConsumption WHERE contentType == 'chat_snap' OR contentType == 'snap'"""
    df = pd.read_sql_query(query_core, con_core)
    return df

def getFriends(database):
    print("Parsing friends")
    con = sqlite3.connect(database)
    query ="""SELECT username as Username,
            userId,
            displayName as Displayname
            from Friend
            """
    df = pd.read_sql_query(query,con)
    for index, row in df.iterrows():
        name = (row['Displayname'])
        fullname = ""
        try:
            if len(name) >= 1:
                for i in name:
                    tmp = i.encode('cp1252', 'xmlcharrefreplace')   #Display Emojis
                    tmp = tmp.decode('cp1252')
                    fullname = fullname + tmp
            else:
                i = fullname[0].encode('cp1252', 'xmlcharrefreplace')
                fullname = i.decode('cp1252')
                
            df.loc[index, "Displayname"] = fullname           
        except:
            continue
           
    return df

def getCache(cachePath):
    print("Getting cached files")
    files = [f for f in os.listdir(cachePath) if os.path.isfile(os.path.join(cachePath, f))]
    for f in files:
        ftype = filetype.guess(cachePath+'//'+f)
        if os.stat(cachePath+'//'+f).st_size != 0 and ftype != None:
            shutil.copy(cachePath+'//'+f, outputDir +'//cacheFiles')
            os.rename(outputDir +'//cacheFiles//'+f,outputDir +'//cacheFiles//'+f.split(".")[0] )
        else:
            files.remove(f)
    
    return files
def joinCache(dataframe, chatfiles, snapfiles):
    filelist = [*chatfiles, *snapfiles]
    for i,j in enumerate(filelist):
        filelist[i] = j.split(".")[0]
    dataframe['hasImage'] = dataframe.cacheKey.isin(filelist)
    return dataframe

def path_to_image_html(filename):
    global attachmentPath_relative
    global exe_path
    global outputDir_name
    path = Path(outputDir + "//cacheFiles//" + filename)
    
    try:
        path = path.replace("\\", "/")
    except Exception:
        pass
    try:
        if os.path.exists(path):
            try:
                basename = ntpath.basename(path)
                realpath = os.path.abspath(path)
                kind = filetype.guess(path)
                if platform == "Windows":
                    relpath = realpath.split("\\")[-2:]
                else:
                    relpath = realpath.split("/")[-2:]
                relpath = str(Path(relpath[0]+"/"+relpath[1]))
                if kind.extension == "mp4":
                    return ('<video width="320" height="240" controls> <source src="' + (relpath) + '" type="video/mp4"> Your browser does not support the video tag. </video> <a href="'+(relpath)+'"><br>'+basename+'</a>')
                elif kind.extension == "png":
                    return ('<a href="' + (relpath) + '"><img src="' + (relpath) + '" width="150" ><br>'+basename+'</a>')
                elif kind.extension == "jpg":
                    return ('<a href="' + (relpath) + '"><img src="' + (relpath) + '" width="150" ><br>'+basename+'</a>')
                else:
                    return filename + " - Unknown extension: " + kind.extension
                
            except Exception as Error:
                print(Error)

                return filename + " missing attachment"
            
        else:
            return filename
    except:
        return

def writeHTML(final_df):
    html = ""
    print("Writing HTML report")
    for index, clientConversationID in final_df.groupby('Client Conversation ID'):
        html = html + clientConversationID.to_html(classes = 'table-striped', escape=False, col_space=100, justify='center', index=False, formatters={'Message Content':path_to_image_html})
    #html = html + friends_df.to_html(classes = 'table-striped', escape=False, col_space=100, justify='center', index=False)
    #html = html + group_df.to_html(classes = 'table-striped', escape=False, col_space=100, justify='center', index=False)

    text_file = open(outputDir + "/report.html", "w", encoding="cp1252")
    text_file.write(html)
    text_file.close()
    print("Success, report can be found in "+ os.path.abspath(outputDir))
def main():
    global outputDir
    global platform

    platform = system()
    if len(sys.argv) <2:
        print("ParseAndroid.py <Snapchat folder>")
        sys.exit()
    snapchatFolder = sys.argv[1]
    outputDir = "./Snapchat_report_" + datetime.datetime.today().strftime('%Y%m%d_%H%M%S')
    os.makedirs(outputDir+"//cacheFiles", exist_ok = True)

    df_core = getCore(snapchatFolder+"/databases/core.db")
    df_friends = getFriends(snapchatFolder+"/databases/main.db")
    files_chat = getCache(snapchatFolder+"/files/file_manager/chat_snap")
    files_snap = getCache(snapchatFolder+"/files/file_manager/snap")
    df_core = joinCache(df_core,files_chat, files_snap)
    #print(df_core[df_core['hasImage'] == True]['contentObjectId'].str[-21:])
    df_core['fileKey'] = df_core[df_core['hasImage'] == True]['contentObjectId'].str[-21:]

    df_chats = getChats(snapchatFolder+"/databases/arroyo.db")
    df_chats['isMedia'] = False
    files = df_core[df_core['fileKey'].notnull()]['fileKey'].values.tolist()

    for index, row in df_chats.iterrows():
        for f in files:
            if f in row['message_content']:
                df_chats.loc[index, 'message_content'] = df_core.loc[df_core['fileKey'] == f, 'cacheKey'].iloc[0]
                df_chats.loc[index, 'isMedia'] = True

    df_chats['content_type'] = np.where(df_chats['content_type']==0, "Chat media",
                                        np.where(df_chats['content_type']==1, "Chat message", np.where(df_chats['content_type']==2, "Snap/reply", "Unknown")))            
    final_df = df_chats[((df_chats['content_type'] == 0)&(df_chats['isMedia']==True))| (df_chats['content_type'] == 1) | ((df_chats['content_type'] == 2)&(df_chats['isMedia']==True))]
    
    final_df = final_df.merge(df_friends, on='userId')
    final_df = final_df.drop(columns='isMedia')
    final_df = final_df.rename(columns={
        'Creation Timestamp': 'Creation Timestamp UTC+0', 'Read Timestamp': 'Read Timestamp UTC+0', 'message_content':'Message Content', 'server_message_id':'Servermessage ID', 'content_type':'Content type',
        'extramessage_content':'Comment','userId':'User ID'})
    final_df = final_df.reindex(['Client Conversation ID','Displayname', 'Message Content','Comment', 'Content type', 'Creation Timestamp UTC+0', 'Read Timestamp UTC+0','Username','User ID', 'Servermessage ID'], axis=1)
    final_df = final_df.sort_values(by=['Client Conversation ID', 'Creation Timestamp UTC+0'])
    writeHTML(final_df)
            
    
if __name__ == "__main__":

    main()
