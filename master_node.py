
import socket		
import pickle	
from subprocess import call
from subprocess import PIPE, Popen, STDOUT, TimeoutExpired

s = socket.socket()		
print ("Socket successfully created")
port = 12345
s.bind(('', port))		
print ("Client Node socket binded to %s" %(port))
s.listen(5)	
print ("Client Node socket is listening")
c, addr = s.accept()	
print ('Got connection from', addr )

print("CLIENT_MASTER")

workers=pickle.loads(c.recv(1024))
sock_name=[]
connection=[]
address=[]
worker_port1=[]
worker_port2=[]
initial_port1=12346
initial_port2=22346

# sock_name=["s1","s2","s3"]
# connection=["c1","c2","c3"...no_of_workers]
# address=["addr1","addr2","addr3"]
# worker_port=[12346,12347,12348]	

for i in range(workers):
    sock_name.append("s"+str(i+1))
    connection.append("c"+str(i+1))
    address.append("addr"+str(i+1))
    worker_port1.append(initial_port1)
    worker_port2.append(initial_port2)
    initial_port1+=1
    # worker to master
    initial_port2+=1 
    # worker to client

i=0
# master to worker
for i in range(workers):  
    sock_name[i]=socket.socket()
    sock_name[i].bind(('', worker_port1[i]))
    print ("W"+str(i),"socket binded to %s" %(worker_port1[i]))
    sock_name[i].listen(5)	
    print ("socket is listening")
    cmd="Python3 {} {} {} {} {}".format("worker.py",i+1,worker_port1[i],worker_port2[i],workers)
    # python3 worker.py 1 12346 22346 no_of_workers
    process_machine = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, text=True)
    # will execute cmd on sep terminal

    connection[i],address[i] = sock_name[i].accept()	
    print ('Got connection from', address[i] )

ack=0
ack1=0
ack2 =0
while True:

    
    data = c.recv(1024)
    recvd=pickle.loads(data)
    print(f"Received {recvd!r}")
    if recvd[0]=="W":
        data=pickle.dumps(worker_port1[0:workers])
        c.send(data)
        for i in range(workers):
            data1=pickle.dumps("START")
            connection[i].send(data1)
            # start command for w sent to workers
    
    if recvd[0]=="R":
        data=pickle.dumps(worker_port1[0:workers])
        c.send(data)
        for i in range(workers):
            data1=pickle.dumps("START1")
            connection[i].send(data1)
    
    elif recvd[0]=="MR":
        for i in range(workers):
            data1=pickle.dumps("START2")
            connection[i].send(data1)
        for i in range(workers):
            data2=pickle.dumps(recvd[1:])
            connection[i].send(data2)
        for i in range(workers):
            msg=connection[i].recv(1024)
            if pickle.loads(msg)=="ACK":
                ack+=1
        if ack==workers:
            print("MAP COMPLETED")
            ack=0
        for i in range(workers):
            data3=pickle.dumps(workers)
            connection[i].send(data3)
        print("worker info sent")
        for i in range(workers):
            msg=connection[i].recv(1024)
            if pickle.loads(msg)=="ACK1":
                ack1+=1
        if ack1==workers:
            print("SHUFFLE COMPLETED")
            ack1=0
        for i in range(workers):
            msg=connection[i].recv(1024)
            if pickle.loads(msg)=="ACK2":
                ack2+=1
        if ack2==workers:
            print("REDUCE COMPLETED")
            print("MAP-REDUCE COMPLETED")
            ack2=0
        f=open("part-0000","w")
        dir="worker"
        for i in range(workers):
            new_dir=dir+str(i+1)+"/part-0000"
            f1=open((new_dir),"r")
            f.write(f1.read())
            f1.close()
            new_dir=''
        f.close()

        pass
    
    if not data:
        break
    try:
        c.sendall(data)
    except:
        break


