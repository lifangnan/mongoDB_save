# Author: 李方楠
# Date: 2022.3.1
# 这一python脚本用于在打靶试验后，将硬件状态相关的PV数据，以及实验产生的复杂类型数据（例如图片、视频）按规定的格式保存在MongoDB数据库中。
# 保存行为的触发来源于DG645触发信号，此程序中由相机状态PV “'13ANDOR1:cam1:NumImagesCounter_RBV”表示实验所处的状态（例如空闲0、图像采集完成1）

# from pvaccess import *
import pymongo
from io import BytesIO
import base64
from PIL import Image
import numpy as np
import pandas as pd
from bson.binary import Binary
import pickle
import time
import datetime
# 获取相机拍摄完成标志
from CaChannel import CaChannel, CaChannelException


list_PV_name = ['IT:PSB1:GetCurren',
                'IT:PSB1:GetVoltag',
                'IT:PSB1:PowerOf',
                'IT:PSB1:PowerO',
                'IT:PSB1:Rese',
                'IT:PSB1:SetCurren',
                'IT:PSB1:Status',
                'IT:PSB1:Status',
                'IT:PSG1:GetCurren',
                'IT:PSG1:GetVoltag',
                'IT:PSG1:PowerOf',
                'IT:PSG1:PowerO',
                'IT:PSG1:Rese',
                'IT:PSG1:SetCurren',
                'IT:PSG1:Status',
                'IT:PSG1:Status',
                'IT:PSG2:GetCurren',
                'IT:PSG2:GetVoltag',
                'IT:PSG2:PowerOf',
                'IT:PSG2:PowerO',
                'IT:PSG2:Rese',
                'IT:PSG2:SetCurren',
                'IT:PSG2:Status',
                'IT:PSG2:Status',
                'IT:PSG3:GetCurren',
                'IT:PSG3:GetVoltag',
                'IT:PSG3:PowerOf',
                'IT:PSG3:PowerO',
                'IT:PSG3:Rese',
                'IT:PSG3:SetCurren',
                'IT:PSG3:Status',
                'IT:PSG3:Status',
                'IT:PSG4:GetCurren',
                'IT:PSG4:GetVoltag',
                'IT:PSG4:PowerOf',
                'IT:PSG4:PowerO',
                'IT:PSG4:Rese',
                'IT:PSG4:SetCurren',
                'IT:PSG4:Status',
                'IT:PSG4:Status',
                'IT:PSQ1:GetCurren',
                'IT:PSQ1:GetVoltag',
                'IT:PSQ1:PowerOf',
                'IT:PSQ1:PowerO',
                'IT:PSQ1:Rese',
                'IT:PSQ1:SetCurren',
                'IT:PSQ1:Status',
                'IT:PSQ1:Status',
                'IT:PSQ2:GetCurren',
                'IT:PSQ2:GetVoltag',
                'IT:PSQ2:PowerOf',
                'IT:PSQ2:PowerO',
                'IT:PSQ2:Rese',
                'IT:PSQ2:SetCurren',
                'IT:PSQ2:Status',
                'IT:PSQ2:Status',
                'IT:PSQ3:GetCurren',
                'IT:PSQ3:GetVoltag',
                'IT:PSQ3:PowerOf',
                'IT:PSQ3:PowerO',
                'IT:PSQ3:Rese',
                'IT:PSQ3:SetCurren',
                'IT:PSQ3:Status',
                'IT:PSQ3:Status',
                'IT:PSQ4:GetCurren',
                'IT:PSQ4:GetVoltag',
                'IT:PSQ4:PowerOf',
                'IT:PSQ4:PowerO',
                'IT:PSQ4:Rese',
                'IT:PSQ4:SetCurren',
                'IT:PSQ4:Status',
                'IT:PSQ4:Status',
                'IT:PSQ5:GetCurren',
                'IT:PSQ5:GetVoltag',
                'IT:PSQ5:PowerOf',
                'IT:PSQ5:PowerO',
                'IT:PSQ5:Rese',
                'IT:PSQ5:SetCurren',
                'IT:PSQ5:Status',
                'IT:PSQ5:Status',
                'Mshutterstat',
                'Mshutterope',
                'Mshutterclo']


# 方法1：向mongoDB插入一个新的event表
def insert_one_event_collection(_db, _title="unnamed", _Triger=None, _list_PV_name=[], _json_Data_list=None):
    json_PV_list = {}
    for PV_name in _list_PV_name:
        PV_VAL = None
        PV_DESC = None

        try:
            temp_VAL_channel = CaChannel(PV_name + ".VAL")
            temp_VAL_channel.searchw()
            PV_VAL = temp_VAL_channel.getw()

            temp_DESC_channel = CaChannel(PV_name + ".DESC")
            temp_DESC_channel.searchw()
            PV_DESC = temp_DESC_channel.getw()
        except CaChannelException as e:
            print(e)

        json_PV_list[PV_name] = {"VAL": PV_VAL, "DESC": PV_DESC}

    event_document = {
        "Title": _title,
        "Timestamp": datetime.datetime.now(),
        "Triger": _Triger,
        "PV_list": json_PV_list,
        "Data_list": _json_Data_list
    }

    event_collection = _db['event']
    event_collection.insert_one(event_document)


# 方法2：读取Andor相机拍到的图片
def read_Andor_and_insert_file(_db, _index_shot = None):
    json_Data_list = None

    is_OK = Chan_NumImagesCounter_RBV.getw()
    time.sleep(0.1)
    print(is_OK)
    Chan_ArrayCounter = CaChannel('13ANDOR1:cam1:ArrayCounter_RBV')
    Chan_ArrayCounter.searchw()
    _index_shot = Chan_ArrayCounter.getw()

    # 像素x和y
    # size_x
    Chan_cam1_SizeX = CaChannel('13ANDOR1:cam1:SizeX')
    Chan_cam1_SizeX.searchw()
    Andorvisionpixelx = Chan_cam1_SizeX.getw()
    # size_y
    Chan_cam1_SizeY = CaChannel('13ANDOR1:cam1:SizeY')
    Chan_cam1_SizeY.searchw()
    Andorvisionpixely = Chan_cam1_SizeY.getw()
    # 曝光时间
    Chan_cam1_AcquireTime = CaChannel('13ANDOR1:cam1:AcquireTime')
    Chan_cam1_AcquireTime.searchw()
    AndorvisionAcquireTime = Chan_cam1_AcquireTime.getw()
    # 存储时间
    #Chan_cam1_time = CaChannel('13ANDOR1:cam1:time')
    # Chan_cam1_time.searchw()
    #Andorvisionfiletime = Chan_cam1_time.getw()

    # 相机数据传输模块
    # Andorvision相机数据传输
    # data

    try:  # 如果相机未正常工作
        Chan_image1_ArrayData = CaChannel('13ANDOR1:image1:ArrayData')
        Chan_image1_ArrayData.searchw()
        pic_arraydata = Chan_image1_ArrayData.getw(use_numpy=True)
        # 保存图片
        pic_arraydata = np.array(pic_arraydata, dtype=np.uint8)
        pic_arraydata = pic_arraydata.reshape(
            Andorvisionpixelx, Andorvisionpixely)
        image_Andor = Image.fromarray(pic_arraydata)  # 可传输灰度图和彩色图
        image_Andor = image_Andor.convert('L')
        # 将Image对象存在内存BytesIO中
        pic_in_memory = BytesIO()
        image_Andor.save(pic_in_memory, format='TIFF')

        file_name = str(_index_shot) + '_ANDOR1.tiff'
        image_Andor_document = {'File_name': file_name, 'Data': base64.encodebytes(
            pic_in_memory.getvalue()), 'pixel_x': Andorvisionpixelx, 'pixel_y': Andorvisionpixely}

        # 将文件上传到MongoDB中，数据使用base64的字节编码
        file_collection = _db['event']
        insert_result = file_collection.insert_one(image_Andor_document)
        file_id = str(insert_result.inserted_id)

        json_Data_list = {file_id: {'File_name': file_name, 'Source': 'ANDOR1',
                                    'File_type': 'TIFF', 'Timestamp': datetime.datetime.now()}}
        # image_Andor.save(str(i) + ".tiff")
    except:
        pass

    return json_Data_list


# 方法2：向mongoDB插入一个新的配置表document，假设传入的参数全部为string类型
# def write_Configuration_document(_db, _Triger_sources_list, _pv_list, _UI_port, _Archiver_port):
#     configuration_document = {
#         "modified_time": datetime.datetime.now(),
#         "triger_sources": eval(_Triger_sources_list),
#         "pv_list": eval(_pv_list),
#         "ui_port": int(_UI_port),
#         "archiver_port": int(_Archiver_port)
#     }
#     configuration_collection = _db['configurations']
#     configuration_collection.insert_one(configuration_document)


# 连接数据库
client = pymongo.MongoClient(
    "mongodb://222.29.111.164:27017/?readPreference=primary&appname=MongoDB%20Compass&ssl=false")
db = client.clapa7
mydb = db.andor1

Chan_NumImagesCounter_RBV = CaChannel('13ANDOR1:cam1:NumImagesCounter_RBV')
Chan_NumImagesCounter_RBV.searchw()


while True:
    is_OK = Chan_NumImagesCounter_RBV.getw()
    while is_OK == 0:
        is_OK = Chan_NumImagesCounter_RBV.getw()
        time.sleep(0.1)

        # 状态位变为1，说明ANDOR相机拍照完成
        if is_OK == 1:
            Chan_ArrayCounter = CaChannel('13ANDOR1:cam1:ArrayCounter_RBV')
            Chan_ArrayCounter.searchw()
            index_shot = Chan_ArrayCounter.getw() # i 代表发次

            json_Data_list = read_Andor_and_insert_file(mydb, index_shot)

            insert_one_event_collection(
                mydb, _title = "No." + str(index_shot), _Triger=None, _list_PV_name = list_PV_name, _json_Data_list = json_Data_list)
