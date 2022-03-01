# Author: 李方楠
# Date: 2022.3.1
# 这一python脚本用于在打靶试验后，将硬件状态相关的PV数据，以及实验产生的复杂类型数据（例如图片、视频）按规定的格式保存在MongoDB数据库中。
# 保存行为的触发来源于DG645触发信号，此程序中由相机状态PV “'13ANDOR1:cam1:NumImagesCounter_RBV”表示实验所处的状态（例如空闲0、图像采集完成1）

# from pvaccess import *
import pymongo
from PIL import Image
import numpy as np
import pandas as pd
from bson.binary import Binary
import pickle
import time
#获取相机拍摄完成标志
from CaChannel import ca, CaChannel


Chan_NumImagesCounter_RBV = CaChannel('13ANDOR1:cam1:NumImagesCounter_RBV')
Chan_NumImagesCounter_RBV.searchw()

while True:   
    is_OK = Chan_NumImagesCounter_RBV.getw()
    while is_OK == 0 :
        is_OK = Chan_NumImagesCounter_RBV.getw()
        time.sleep(0.1)
        
        if is_OK == 1:
            # is_OK = Chan_NumImagesCounter_RBV.getw()
            time.sleep(0.1)
            print (is_OK)
            Chan_ArrayCounter = CaChannel('13ANDOR1:cam1:ArrayCounter_RBV')
            Chan_ArrayCounter.searchw()
            i = Chan_ArrayCounter.getw()
            #相机数据传输模块
            #Andorvision相机数据传输
            #data
            Chan_image1_ArrayData = CaChannel('13ANDOR1:image1:ArrayData')
            Chan_image1_ArrayData.searchw()
            data1 = Chan_image1_ArrayData.getw(use_numpy=True)
            #像素x和y
            # size_x
            Chan_cam1_SizeX = CaChannel('13ANDOR1:cam1:SizeX')
            Chan_cam1_SizeX.searchw()
            Andorvisionpixelx = Chan_cam1_SizeX.getw()
            # size_y
            Chan_cam1_SizeY = CaChannel('13ANDOR1:cam1:SizeY')
            Chan_cam1_SizeY.searchw()
            Andorvisionpixely=Chan_cam1_SizeY.getw()
            #曝光时间
            Chan_cam1_AcquireTime = CaChannel('13ANDOR1:cam1:AcquireTime')
            Chan_cam1_AcquireTime.searchw()
            AndorvisionAcquireTime = Chan_cam1_AcquireTime.getw()
            #存储时间
            #Chan_cam1_time = CaChannel('13ANDOR1:cam1:time')
            #Chan_cam1_time.searchw()
            #Andorvisionfiletime = Chan_cam1_time.getw()
            
            #保存备份txt文件
            np.savetxt(str(i) + ".txt", data1, fmt='%d')
            print(data1)
            #a = np.loadtxt('str(i).txt')
            #创建数据库
            client = pymongo.MongoClient()
            db = client.clapa7
            mydb = db.andor1
            #在数据库中插入大文件
            mydb.insert_one({'filename': str(i) + ".txt",'pixelx':Andorvisionpixelx, 'pixely':Andorvisionpixely,'data' : Binary(pickle.dumps(data1, protocol=-1), subtype=128)})
            

def insert_event_collection():
    return 0