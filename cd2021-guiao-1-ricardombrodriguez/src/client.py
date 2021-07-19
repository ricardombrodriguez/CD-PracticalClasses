"""CD Chat client program"""
import logging
import socket
import selectors
import sys
import fcntl
import os
from datetime import datetime,timedelta
from .protocol import CDProto, CDProtoBadFormat


logging.basicConfig(filename=f"{sys.argv[0]}.log", level=logging.DEBUG)

class Client:

    def __init__(self, name: str = "Foo"):
        self.name = name
        self.channel = None
        self.conn = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.sel = selectors.DefaultSelector()
        self.addr = ('localhost',5000)
        self.CDProto = CDProto()


    def connect(self):
        self.conn.connect(self.addr)         
        self.sel.register(self.conn, selectors.EVENT_READ, self.read)    
        regist = self.CDProto.register(self.name)   #RegisterMessage
        self.CDProto.send_msg(self.conn, regist)


    def read(self,conn,mask):
        messageObj = self.CDProto.recv_msg(self.conn)                                   #calls protocol recv_msg
        if messageObj.command == "message":                                             #log
            logging.debug('received "%s"', messageObj.message)
        msgTime = datetime.utcfromtimestamp(messageObj.ts + 60*60).strftime('%H:%M:%S') #converts timestamp to gmt time (h:m:s format)
        print(f"\033[92m({msgTime}) {messageObj.message} \033[0m")                      #received messages are presented in green


    def getInput(self,stdin,mask):

        textInput = stdin.read()
        if (textInput == "exit\n"):                                     #Disconnect
            sys.exit("!!!!! DISCONNECTED !!!!!")

        elif (textInput[:5] == "/join"):                                #JoinMessage
            self.channel = textInput[6:-1]                              #channel name

            if (len(self.channel.split(' ')) == 1):                     #only allows one word for channel name
                join_msg = self.CDProto.join(self.channel)              #creates Join type object
                join_send = self.CDProto.send_msg(self.conn, join_msg)  #tells other clients that the user has joined the channel
            else:
                print("Invalid channel name. Try again.")

        else:                                                           #TextMessage
            msg = self.CDProto.message(textInput[:-1],self.channel)     #creates Text type objectt
            msg_recebida = self.CDProto.send_msg(self.conn, msg)        #sends message for other users


    def loop(self):

        print(f"!!!!! [{self.name.upper()}] CONNECTED !!!!!!")
        orig_fl = fcntl.fcntl(sys.stdin, fcntl.F_GETFL)
        fcntl.fcntl(sys.stdin, fcntl.F_SETFL, orig_fl | os.O_NONBLOCK)
        self.sel.register(sys.stdin, selectors.EVENT_READ, self.getInput)

        while True:
            if (self.channel == None):                                  #present the channel name colored in green
                sys.stdout.write("\033[92m[#Broadcast]\033[0m ")
            else:
                sys.stdout.write(f"\033[92m[#{self.channel}]\033[0m ")
            sys.stdout.flush()
            for k, mask in self.sel.select():
                callback = k.data
                callback(k.fileobj,mask)

        