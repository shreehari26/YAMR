import socket	
import pickle
from subprocess import run
import sys
import os

w=int(sys.argv[1])
port=int(sys.argv[2])
port_c=int(sys.argv[3])
no=int(sys.argv[4])




def callpy(x,path,save1):
            run("Python3 {} {} {}".format(path,str(save1),x),input=x.encode())

def callshuffle(x,path,save1,no_wrk):
    run("Python3 {} {} {} {}".format(path,str(save1),no_wrk,x),input=x.encode())

def callred(x,path,save1):
    run("Python3 {} {} {}".format(path,str(save1),x),input=x.encode())

s = socket.socket()		



path_to_shuffle="map-red/shuffle.py"

s.connect(('127.0.0.1', port))
print("worker"+str(w)+" master")

con=1
c = socket.socket()
c.connect(('127.0.0.1', port_c))
print("worker"+str(w)+" Client")
con=0

path_to_folder="C:/pes2ug20cs284/sem5/big data/big data project/BD2_198_214_284_291"
folder_name="worker"+str(w)

path = os.path.join(path_to_folder, folder_name)
os.mkdir(path)
print("Worker"+str(w)+" created directory "'% s'% folder_name)



while True:
    
    x=pickle.loads(s.recv(1024))
    print (x)
    if x=="START":
        print("hi")
        data=c.recv(1024) #partitioned data recieved
        data=pickle.loads(data)
        print(data[0])#path
        x=data[0]
        l=x.split(".")
        new_x="worker"+str(w)+"/"+l[0]+str(w)+"."+l[1]
        with open(new_x, "w") as output:
            for i in data[1]:
                output.write(i)
    
    elif x=="START1":
        data=c.recv(1024)
        data=pickle.loads(data)
        print(data)
        l=data.split(".")
        new_x="worker"+str(w)+"/"+l[0]+str(w)+"."+l[1]
        f=open(new_x,"r")
        info=f.readlines()
        c.send(pickle.dumps(info))

    elif x=="START2":
        l=pickle.loads(s.recv(1024))
        path_to_w=l[0]
        path_to_w=path_to_w.split(".")
        l[0]="worker"+str(w)+"/"+path_to_w[0]+str(w)+"."+path_to_w[1]
        save="worker"+str(w)+"/"+path_to_w[0]+str(w)+"partial"+"."+path_to_w[1]
        f=open(l[0],"r")
        info=f.read()
        # input.txt --> input1.txt   def input1_partial.txt and reading input1.txt
        callpy(info,l[1],save)  #l1 is path to mapper and save where we want to save ie inpu1_partial.txt
        s.send(pickle.dumps("ACK"))
        no_wrk=pickle.loads(s.recv(1024))
        print(no_wrk)
        shuffle_save="worker/"+path_to_w[0]+"shuffle"+"."+path_to_w[1]
        new_f=open(save,"r")
        new_info=new_f.read()
        callshuffle(new_info,path_to_shuffle,shuffle_save,no_wrk)
        s.send(pickle.dumps("ACK1"))
        save_shuffle="worker"+str(w)+"/"+path_to_w[0]+str(w)+"shuffle"+"."+path_to_w[1]
        f_final=open(save_shuffle,"r")
        x = f_final.readlines()
        x.sort()
        new_x=""
        for i in x:
            new_x+=i
        callred(new_x,l[2],"worker"+str(w)+"/part-0000")
        s.send(pickle.dumps("ACK2"))
        s.send(pickle.dumps("worker"+str(w)+"/part-0000"))
   
s.close()	

