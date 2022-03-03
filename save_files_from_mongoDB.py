# Author: 李方楠
# Date: 2022.3.3


import pymongo
from io import BytesIO
import base64
from bson import ObjectId
import os

# 链接mongoDB
def get_mongoDB_collection(mongoDB_link = None, CollectionName = "clapa7"):
    client = pymongo.MongoClient(mongoDB_link)
    collection = client[CollectionName]
    return collection

# 提取event表中，指定title的发次下的所有PV data
def get_PVdata_with_title(_db, _title):
    collection = _db['event']
    event_document = collection.find_one({'Title': _title})
    if event_document == None:
        print("MongoDB中没有找到对应Title的数据记录。")
        return None
    
    PV_data_dict = event_document['PV_list']
    return PV_data_dict
    

# 提取event表中，指定title的发次下的所有文件数据，经过base64解码保存至本地，以event命名的文件夹中
def get_files_with_title(_db, _title, _SavePath='./'):
    collection = _db['event']
    file_collection = _db['files']
    
    event_document = collection.find_one({'Title': _title})
    if event_document == None:
        print("MongoDB中没有找到对应Title的数据记录。")
        return
    
    file_list = event_document['Data_list']
    
    # 在指定的保存路径下创建event相应文件夹
    if not os.path.exists(os.path.join(_SavePath, _title)):
        os.mkdir(os.path.join(_SavePath, _title))
    
    for file_id in file_list.keys():
        file_document = file_collection.find_one( ObjectId(file_id) )
        
        file_name = file_document['File_name']
        file_data = base64.b64decode( file_document['Data'] )
        print("Get file: ", file_name)
            
        with open(os.path.join(_SavePath, _title, file_name), 'wb') as f:
            f.write(file_data)
    

db = get_mongoDB_collection()
PV_data_dict = get_PVdata_with_title(db, 'No.2.0')
get_files_with_title(db, 'No.2.0', '..')