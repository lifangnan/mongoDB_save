# mongoDB_save

## 1. 文件Mongo_save.py
这是一个while True，保持运行的脚本。当ANDOR相机完成拍摄，相应状态位PV（'13ANDOR1:cam1:NumImagesCounter_RBV'）变为1后，触发自动化存储数据的功能。
* 首先在mongoDB的event表中生成一条实验记录。Title对应发次，Timestamp为保存时间datetime，PV_list保存了需要记录的所有PV的VAL和DESC，Data_list保存了文件名、文件在file表中存储的_id、文件格式、文件来源等信息。
* file表中保存了多条文件记录，每条记录包括文件名、经过base64模块的b64编码后的二进制数据，等其他信息。例如ANDOR相机产生的图片记录中，还保存了横纵像素值的信息。


## 2. 文件save_files_from_mongoDB.py
这是一个脚本文件，在需要时运行，根据实际情况选择性地调用其中的方法。主要实现了以下两个功能。
* 提取event表中，指定title的发次下的所有PV data。
    '''
    db = get_mongoDB_collection()
    PV_data_dict = get_PVdata_with_title(db, 'No.2.0')
    '''

* 提取event表中，指定title的发次下的所有文件数据，经过base64解码保存至本地指定路径，以event命名的文件夹中
    '''
    get_files_with_title(db, 'No.2.0', '..')
    '''