# encoding: utf-8
import socketio
import os
import threading
import queue
import time
from multiprocessing import Process, Lock
from collections import Counter
import sys
import time
import logging


# 以下数据重置任务后需要重新清空
# start
# 存储服务器发送的流数据的队列
q = queue.Queue()
# 存储客户端计算机MASH计算后的结果
q2 = queue.Queue()
# 最后结果
result_dic = {}
# 需要下载的文件信息
uuid = ""
file_name = ""
sign_boolean = False
con = threading.Condition()
end_sign = [False, False, False, False]
# end
# standard Python
sio = socketio.Client()

# 本地文件位置
MASH = "ReferenceSeqData/mash"
DefaultREFDB = "ReferenceSeqData/RefSeqSketchesDefaults.msh"
REFDB = "ReferenceSeqData/RefSeqSketches.msh"
# 取MASH结果的前N进行保留
topn = 3
buffer_size = 10485760


def getEnd_sign(_end_sign):
    """
    判断任务是否结束
    """
    for i in _end_sign:
        if ~i:
            return False
    return True


@sio.on('files_list')
def receive_files(files):
    """
    接收文件列表
    """
    for file_info in files:
        temp_foo = file_info.split(" ")
        print("{:<10} {:<10} {:<10}".format(
            temp_foo[0], temp_foo[1], temp_foo[2]))
    temp = input("Please enter ID which you hope handle file：")
    sio.emit("start_receive", temp)


@sio.event
def message(data):
    # 保存接收文件信息
    print("test 接收到信息了{0}".format(data))
    global uuid, file_name, sign_boolean
    uuid = str(data["uuid"])
    file_name = str(data["file_name"])
    sign_boolean = True
    foo_temp1 = str(os.path.join("temp", str(uuid)+"_temp_file"))
    try:
        # 创建消费者（处理）和生产着线程（写入）的线程
        # 生产者线程
        print("start to receive file :{0}".format(file_name))
        t1 = threading.Thread(
            target=write_fastq, args=(foo_temp1,))
        t1.start()
        # 次级消费者（高级生产者）
        t2 = threading.Thread(
            target=handle_fastq, args=(foo_temp1,))
        t2.start()
        # 最高级消费者
        t3 = threading.Thread(
            target=handle_result, args=())
        t3.start()
        t4 = threading.Thread(
            target=destruction, args=(t1, t2, t3,))
        t4.start()
    except BaseException:
        print("Error: unable to start thread")


def destruction(*t):
    while True:
        time.sleep(20)
        print(end_sign)
        if(getEnd_sign(end_sign)):
            for i in t:
                i._Thread__stop()
            return


@sio.on('file_content')
def file_content(data):
    """
    接收文件内容
    """

    if(data is "end"):
        end_sign[0] = True
        return
    # 因为服务器是多线程传输，像数据链路层一样，分包传输，最后在这边写入时多线程写入会造成数据丢失。因此先放入队列里
    # print("test", data)
    q.put(data)


def write_fastq(foo_file):
    """
    生产者线程,将接收到的数据写入
    """
    with open(foo_file, "a+") as f:
        with open(foo_file+"2", "a+") as f2:
            print("start to process file :{0}".format(file_name))
            global sign_boolean
            global end_sign
            while True:
                if sign_boolean:
                    if not q.empty():
                        # 强行将数据放进去
                        s_foo = q.get()
                        f.write(s_foo)
                        f2.write(s_foo)
                        f.flush()
                        f2.flush()
                        # print("test ,开始flush")
                    # 如果大于10MB,停止生产,将数据都放入队列里,等待消费者（处理程序，MASH）消费
                    if(int(os.path.getsize(foo_file)) >= int(buffer_size)):
                        with con:
                            print(
                                "[stream chuck write end]Cache hava enough data (10MB)")
                            con.notify()
                            # 等待消费者消费完的信号
                            con.wait()
                    # 传输完成后最后一次不足10M
                    if(end_sign[0] and q.empty() and int(os.path.getsize(foo_file)) <= int(buffer_size)):
                        with con:
                            print(
                                "Write Done")
                            con.notify()
                        end_sign[1] = True
                        return


def handle_fastq(foo_file):
    """
    消费者处理数据
    """
    while True:
        # 如果数据达到Buffer要求
        if (os.path.exists(foo_file)):
            with con:
                con.wait()
                # 进行MASh计算
                print(
                    "start to compute stream chuck :{0}".format(file_name))
                dist_result = os.popen(MASH + "  dist  "
                                       + DefaultREFDB + " "+foo_file)
                lines = dist_result.readlines()
                # 消费完毕,清除消费的数据。
                with open(foo_file, "r+") as f:
                    f.truncate()
                # 告诉生产者（写入线程）继续写入
                print("[stream chuck handle end]")
                con.notify()
                # 处理MASH后的结果。list for mash result
                lines_list = list()
                for line in lines:
                    lines_list.append(line.split("\t"))
                # sort result by mash distence 0.1 0.3 0.5 0.88
                lines_list.sort(key=lambda i: i[2])
                # convert to a dictory whose  key is the name of specie gene bank and value is mash distence
                _dic = {}
                for line in lines_list[:int(topn)]:
                    if line[2] != '1':
                        # _dic[line[0]] = float(line[2])
                        _dic[line[0]] = 1
                q2.put(_dic)
                if(end_sign[1]):
                    print("Handle Done")
                    end_sign[2] = True
                    return


def handle_result():
    """
    将每次的结果进行不同方式，不同权重积累计算,高级消费者
    """
    global result_dic
    count = 1
    while True:
        if not q2.empty():
            # accumulative results
            print(
                "start to accumulative stream chuck  result:{0}".format(file_name))
            re = q2.get()
            result_dic = dict(Counter(result_dic)+Counter(re))
            # sys.stdout.writelines(result_dic)
            for (key, value) in result_dic.items():
                print("{:<30} : {:<8}".format(key, value))
            count += 1
            time.sleep(1)
        if(end_sign[1] and q2.empty):
            print("Accumulate Done")
            end_sign[3] = True
            return


if __name__ == '__main__':
    if not os.path.isdir("temp"):
        os.makedirs("temp")
    sio.connect('http://localhost:5000')
