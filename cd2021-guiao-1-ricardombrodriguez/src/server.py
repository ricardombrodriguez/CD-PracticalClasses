import logging
import socket
import selectors
import json
import datetime
from .protocol import CDProto, CDProtoBadFormat
from collections import defaultdict

logging.basicConfig(filename="server.log", level=logging.DEBUG)


class Server:

    def __init__(self):
        print("[SERVER] Creating server...")
        self.server = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.server.bind(('localhost',5000))
        self.sel = selectors.DefaultSelector()
        self.server.listen()
        self.sel.register(self.server, selectors.EVENT_READ, self.accept)
        self.member_list = defaultdict(list)          #connection : [channel]
        self.socket_names = {}                        #connection : username


    def accept(self,server,mask):
        conn, addr = server.accept()
        print(f"[SERVER] Connection estabilished with {addr[0]} | Port: {addr[1]}")
        conn.setblocking(False)
        self.sel.register(conn, selectors.EVENT_READ, self.read)


    def read(self,conn, mask):
        data = CDProto.recv_msg(conn)       
        if data:                                                                                        # if there's a message
            if (data.command == "Register"):                                                            #add channel 'None' to connection key
                username = data.username
                self.member_list[conn] = [None]
                self.socket_names[conn] = username
                logging.debug('connected %s', username)
        
            elif (data.command == "Join"):                                                              #join a channel

                channel = data.channel
                if (self.member_list[conn] == [None]):                                                  #1st channel join
                    self.member_list[conn] = [channel]
                    obj = CDProto.message(f"{self.socket_names[conn]} has joined {channel}.",channel)
                    logging.debug('user ' + self.socket_names[conn] + ' joined ' + channel)

                    for member,channelList in self.member_list.items():
                        if (channel in channelList):
                            CDProto.send_msg(member,obj)

                elif (channel in self.member_list[conn]):                                               #if the user's already in that channel
                    pass

                else:                                                                                   #join a new channel
                    self.member_list[conn].append(channel)
                    obj = CDProto.message(f"{self.socket_names[conn]} has joined #{channel}.",channel)
                    logging.debug('user ' + self.socket_names[conn] + ' joined ' + channel)

                    for member,channelList in self.member_list.items():
                        if (channel in channelList):
                            CDProto.send_msg(member,obj)

            elif (data.command == "Text"):                                                               #sends text message

                msg = f"{self.socket_names[conn]}: {data.message}"
                channel = data.channel
                obj = CDProto.message(msg,channel)
                logging.debug('text message: ' + msg)

                for member,channelList in self.member_list.items():
                    if (member != conn and channel in channelList):
                        CDProto.send_msg(member,obj)

        else:                                                       #user disconnects (remove from dict and close connection)                                     
            self.member_list.pop(conn)
            self.sel.unregister(conn)
            conn.close()

    def loop(self):
        while True:
            events = self.sel.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)