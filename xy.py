# coding:utf-8

import rpyc
from rpyc import Service
from rpyc.utils.server import ThreadedServer
from importlib import import_module
import threading
import queue
import time
import random
import os
import json

# module=import_module('xxy')
# s = getattr(module, 'test')
# s()

# 服务器中途断开时客户端会报错，客户端可以添加一个检测程序
class TimeService(Service):
    def __init__(self):
        self.mess_statu = '服务器空闲！'

    def exposed_Test(self):
        str = 'This is a test code, you have connected to the server, Thank you! ~*^*~'
        return str

    def exposed_task_rcv(self,inf,data):             # 任务接收
        p_r.Producer(inf,data)

    def exposed_status_inquiry(self,task_num):           # 状态查询
        # if mess_queue.empty() & (self.mess_statu!='正在处理！'):
        #     self.mess_statu = '服务器空闲！'
        try:
            task_statu = task_inquiry[task_num]['statu']
            img_path = task_inquiry[task_num]['img_path']
        except:
            task_statu = '未接到任务信息！'
            img_path = ''
        return task_statu,img_path

    def exposed_data_down(self,img_path):                # 返回结果
        with open(img_path,'rb') as f:
            str_img = f.read()
        return str_img


# 任务处理完成有通知，并给客户端回传
# 消息处理完成怎么告诉客户端，当有多个用户同时连接的时候怎么讲结果准确反馈给正确用户
# 队列的消息存储，消息占用内存大应该不能直接存储，那么这么让消费者完成这一任务，也就是exposed_Producer占用时间很少
# exposed_Producer花的时间越少越能抗住大流量的冲击
# 线程通信与等待
# 消息队列为空则等待，数量过多可以给客户端反馈
# 生产者判断消息队列是否为空？如果为空则告诉消费者休眠
# 还是说消费者自己检查队列是否为空，一直死循环检查？
# 两者哪个更优？


# 需要做一个测试，当有多个客户端连接服务器时是否会出现变量，任务混乱
class Producer_Consumer():
    def __init__(self,mess_queue,envent):
        self.mess_queue = mess_queue
        self.envent = envent
        self.envent.clear()     # 设置为False, 让线程阻塞
        # 线程的暂停，恢复与退出
        # https://www.cnblogs.com/scolia/p/6132950.html
        # https://www.cnblogs.com/xiaokang01/p/9096475.html

    def Producer(self,mess,img):
        dic_mess = json.loads(mess)
        queue_code = dic_mess['task_num']
        img_num = dic_mess['img_num']

        task_inquiry[queue_code] = {'statu': '正在提交！',
                                    'img_path': ''}
        try:
            self.mess_queue.put(queue_code)         # 放入队列

            local_path = os.getcwd()
            self.task_path = local_path + '/task_json'
            if not os.path.exists(self.task_path):
                os.makedirs(self.task_path)
            # 将消息存储到以队列编码命名的json文件里
            with open(self.task_path+'/%s.json'%queue_code,'w') as f:
                json.dump(mess,f)

            # 将图像以二进制流格式存储到txt文档里
            # 是否可以不存放到硬盘里，系统有足够内存存放这些数据？如果可以怎么区分哪些数据对应哪个任务？
            if img_num == 1:
                with open(self.task_path+'/%s.txt'%queue_code,'wb') as f:
                    f.write(img)
            else:
                for i in range(img_num):
                    with open(self.task_path+'/%s_%d.txt'%(queue_code,i+1),'wb') as f:
                        f.write(img[i])

            task_inquiry[queue_code] = {'statu': '等待处理！',
                                        'img_path': ''}
            self.envent.set()       # 设置为True, 让消费者线程停止阻塞

        except:             # 要写提交失败的原因吗
            task_inquiry[queue_code] = {'statu': '提交失败！',
                                        'img_path': ''}

    def Consumer(self):
        while True:         # 消费者死循环，一直处理或等待处理消息
            print('Sapper: Waiting....')
            self.envent.wait()      # 为True时立即返回, 为False时阻塞直到内部的标识位为True后返回
            print('Sapper: Come on!')

            task_code = self.mess_queue.get()
            task_inquiry[task_code] = {'statu': '正在处理！',
                                        'img_path': ''}

            with open(self.task_path + '/%s.json' % task_code, 'r') as f:
                task_mess = json.load(f)

            # 处理任务....
            save_path,deal_statu = task_deal(task_mess)

            task_inquiry[task_code] = {'statu': deal_statu,
                                        'img_path': save_path}

            # 判断生产者在处理完此条消息时队列里是否还有任务
            if self.mess_queue.empty():
                self.envent.clear()

    def get_fun_args(self,func):        # 获取函数参数个数及参数名称
        num_args = func.__code__.co_argcount
        args = func.__code__.co_varnames
        return num_args,args

def task_deal(json_mess):
    dic_mess = json.loads(json_mess)

    Img_num = dic_mess['img_num']
    task_code = dic_mess['task_num']
    Func_name = dic_mess['dic_info']['task_model']
    para = dic_mess['dic_info']['para']

    dic_inf = {'提交用户':dic_mess['user_name'],
               '文件路径':dic_mess['result_path'],
               '数据格式':''}

    if Img_num == 1:
        with open(p_r.task_path + '%s.txt'%task_code,'rb') as f:
            img = f.read()

        # 调用server_algorithmic模块里的函数
        module=import_module('server_algorithmic')
        algori = getattr(module, Func_name)         # 获得函数名
        save_path,deal_statu = algori(img,dic_inf,*para)            # 运行函数

        return save_path, deal_statu

    elif Img_num == 2:
        with open(p_r.task_path + '%s_1.txt' % task_code, 'rb') as f:
            img1 = f.read()
        with open(p_r.task_path + '%s_2.txt' % task_code, 'rb') as f:
            img2 = f.read()

        # 调用server_algorithmic模块里的函数
        module = import_module('server_algorithmic')
        algori = getattr(module, Func_name)         # 获得函数名
        save_path,deal_statu = algori(img1,img2, dic_inf, *para)            # 运行函数

        return save_path, deal_statu
    # 目前算法里边还没有用到输入3张图像的，暂时先写到Img_num == 2


def createThreadServer():
    s = ThreadedServer(TimeService, port=12222, auto_register=False, protocol_config=rpyc.core.protocol.DEFAULT_CONFIG)
    print('\nSignalman: Start----->')
    s.start()
# 如果数据已经处理过就直接从本地读取结果回传，不要再处理了
# 服务器另一个线程调用客户端程序读取数据。这个目前没有好的解决方案，先不做

if __name__ == "__main__":

    # 数据格式定义，所有数据以字典表示，转为json后发送过来
    # task_info = {
    #     'task_num': '',       # 任务编号
    #     'user_name': '',      # 提交用户
    #     'result_path': '',    # 结果路径，暂时没用
    #     'img_num': int,       # 图像数量
    #     # 嵌套字典，存储被调用函数信息
    #     'dic_info': {'task_type': '',     # 任务类型
    #                  'task_model': '',    # 函数名称
    #                  'para': ('','','','',''),         # 函数参数列表
    #                  }
    # }
    
    task_inquiry = {}           # 定义一个任务查询字典，每增一个任务都嵌入一个相应的子字典，存储各自的任务状态及结果路径
    mess_queue = queue.Queue(0)
    envent = threading.Event()

    p_r = Producer_Consumer(mess_queue,envent)
    T_S = TimeService()

    Th0 = threading.Thread(target=createThreadServer, )     # 接收任务信息和数据。拆分。拆分和不拆分效果一样，先不做
    Th1 = threading.Thread(target=p_r.Consumer,)            # 处理任务

    Th0.start()
    Th1.start()





