
import socket	
import pickle
import math	
import shutil
import os	


s = socket.socket()		
port = 12345			
s.connect(('127.0.0.1', port))
print("CLIENT_MASTER")
status= True

workers=int(input("Enter number of workers:"))
s.send(pickle.dumps(workers))

# sock_name=["s1","s2","s3"]
# connection=["c1","c2","c3"]
# address=["addr1","addr2","addr3"]
# worker_port=[22346,22347,22348]

sock_name=[]
connection=[]
address=[]
worker_port=[]
initial_port=22346	

for i in range(workers):
    sock_name.append("s"+str(i+1))
    connection.append("c"+str(i+1))
    address.append("addr"+str(i+1))
    worker_port.append(initial_port)
    initial_port+=1

for i in range(0,workers):
    sock_name[i]=socket.socket()
    sock_name[i].bind(('', worker_port[i]))
    print ("socket binded to %s" %(worker_port[i]))
    sock_name[i].listen(5)	
    print ("socket is listening")
    connection[i],address[i] = sock_name[i].accept()	
    print ('Got connection from', address[i] )


def partition(worker,path_to_file):
    f= open(path_to_file,"r")
    x=f.readlines()
    lines_per_partition=math.ceil(len(x)/len(worker))
    list_partition=[]
    for i in range(len(worker)):
        list_partition.append(x[:lines_per_partition])
        x=x[lines_per_partition:]
    return list_partition
status1=1



while status==True:
    
    print("1->WRITE\n2->READ\n3->MAP-REDUCE\n4->QUIT")
    x=int(input("Enter choice here:"))
    if x==1:
        operation="W"
        path_to_input=input("ENTER path to input file:")            
        data=pickle.dumps([operation,path_to_input])
        s.send(data)
        data=s.recv(1024)
        data1=pickle.loads(data)
        print(data1)
        # printing list of worker ports (master worker ports)
        
        info = partition(data1,path_to_input)
        for i in range(len(data1)):
            connection[i].send(pickle.dumps([path_to_input,info[i]]))
        data1=[]


    elif x==2:
        operation="R"
        path_to_input=input("ENTER path to input file:")
        data=pickle.dumps([operation,path_to_input])
        s.send(data)
        data1=pickle.loads(s.recv(1024)) #worker nodes
        
        for i in range(len(data1)):
            connection[i].send(pickle.dumps(path_to_input))
        
        print("WORKS")
        for i in range(len(data1)):
            to_display=connection[i].recv(1024)
            to_display=pickle.loads(to_display)
            for j in to_display:
                print(j.strip("\n"))
        data1=[]

    elif x==3:
        operation="MR"
        path_to_input=input("Enter path to input file:")
        path_to_mapper=input("Enter path to mapper file:")
        path_to_reducer=input("Enter path to reducer file:")
        data=pickle.dumps([operation,path_to_input,path_to_mapper,path_to_reducer])
        s.send(data)
    else:
        data=pickle.dumps(["BYE"])
        s.send(data)
        status=False
    

s.close()	
	
