#encoding: utf-8
import eventlet
import socketio
import os
import time
import uuid
from multiprocessing import Process, Lock
from threading import Timer
import sys
import threading
sio = socketio.Server()
app = socketio.WSGIApp(sio, static_files={
    # '/': {'content_type': 'text/html', 'filename': 'index.html'}
})
# 服务器储存文件位置。
file_local_dir = "/root/fastqdir"
# 客户端数量
client_number = 0
l = Lock()
transmission_n_size = 50 * 4
# 缓存本地file
file_list = list()


def read_in_chunks(foo1, session_foo, chunk_size=transmission_n_size):
    send_content = ""
    while True:
        line = foo1.readline()
        send_content += line
        session_foo["current_flie_line_cursor"] += 1
        # 如果到达文件末尾,且不满足N行发送条件的，将剩下的发送。
        if not line:
            if not (session_foo["current_flie_line_cursor"] % chunk_size == 0 and session_foo["current_flie_line_cursor"] >= chunk_size):
                yield send_content
                break
        # 满足N行才传送
        if(session_foo["current_flie_line_cursor"] % chunk_size == 0 and session_foo["current_flie_line_cursor"] >= chunk_size):
            yield send_content
            send_content = ""


def send_fileContent_thread(sio_re, handle_dir, session):
    """
    发送文件内容的线程
    """

    f = open(handle_dir, "r", encoding='utf-8')
    for line in read_in_chunks(f, session):
        sio.emit('file_content', line)
        sio.sleep(2)
    sio.emit('file_content', "end")
    f.close()


class My_File(object):
    """
    文件类
    """

    def __init__(self, ID, file_name, size):
        self.ID = ID
        self.file_name = file_name
        self.size = size

    def __str__(self):
        return ("{0} {1} {2}".format(self.ID, self.file_name, self.size))


@sio.event
def connect(sid, environ):
    global client_number
    # 控制客户端数量
    if client_number > 32:
        sio.emit('message', "client number is too much")
        raise ConnectionRefusedError('client number is too much')
    # 发送文件列表
    # 加锁
    l.acquire()
    client_number += 1
    # 解锁
    l.release()
    sio.save_session(
        sid, {'handle_flie': "", "current_flie_line_cursor": 0, "uuid": str(uuid.uuid1())})
    sio.emit('files_list', file_list)


@sio.event
def disconnect(sid):
    """
    客户端断开连接
    """
    global client_number
    l.acquire()
    client_number -= 1
    l.release()
    print('disconnect ', sid)


@sio.on("start_receive")
def send_fileContent(sid, temp):
    print("要处理文件ID为:", temp)
    # 根据temp的值打开相应的文件，开始发送。
    handle_dir = get_file_dir(temp)
    print("开始发送文件:", handle_dir)
    session = sio.get_session(sid)
    session["handle_flie"] = handle_dir
    # print("test 发送message，包括uuid和文件名")
    sio.emit('message', {
             "uuid": session["uuid"], "file_name": handle_dir})
    # print("test 发送message，包括uuid和文件名  end")
    # 启动一个线程去发送文件内容
    sio.start_background_task(
        send_fileContent_thread, *(sio, handle_dir, session,))


def get_file_list():
    """
    处理服务器文件列表
    """
    global file_list
    file_list.clear()
    file_list.append(str(My_File("ID", "File_Name", "File_Size")))
    for index, fileInfo in enumerate(os.popen("ls -lh " + file_local_dir).readlines()[1:]):
        temp = fileInfo.split(" ")
        temp_file = str(My_File(index, temp[-1].replace("\n", ""), temp[4]))
        file_list.append(temp_file)


def get_file_dir(temp):
    """
    根据temp获取要处理的文件绝对路径
    """
    for file_foo in file_list[1:]:
        file_temp = file_foo.split(" ")
        if int(temp) == int(file_temp[0]):
            return str(os.path.join(file_local_dir, file_temp[1]))


# def printInfo_stdout_thread(*data):
#     while True:
#         print('total_client={}'.format(str(client_number)) +
#               '\n', end='', flush=True)
#         time.sleep(2)


# def send_fileContent_thread():
#     with open(handle_dir, "r", encoding='utf-8') as f:
#         while True:
#             line = f.readline()
#             send_content += line
#             session["current_flie_line_cursor"] += 1
#             # 如果到达文件末尾,且不满足N行发送条件的，将剩下的发送。
#             if not line:
#                 if ~(session["current_flie_line_cursor"] % 4 * transmission_n_read == 0 and session["current_flie_line_cursor"] >= 4*transmission_n_read):
#                     sio.emit('file_content', {
#                         "content": send_content, "uuid": session["uuid"]})
#                     print("test 到达文件末尾")
#                     break
#             # 满足N行才传送
#             if(session["current_flie_line_cursor"] % 4 * transmission_n_read == 0 and session["current_flie_line_cursor"] >= 4*transmission_n_read):
#                 sio.emit('file_content', {
#                          "content": send_content, "uuid": session["uuid"], "file_name": handle_dir})
#                 print("test 发送了正常的4N行数据")
#                 send_content = ""


if __name__ == '__main__':
    # 定时更新服务器文件列表
    get_file_list()
    t = Timer(1800.0, get_file_list)
    t.start()
    for file_info in file_list:
        temp_foo = file_info.split(" ")
        print("{:<10} {:<10} {:<10}".format(
            temp_foo[0], temp_foo[1], temp_foo[2]))
    eventlet.wsgi.server(eventlet.listen(('', 5000)), app)
