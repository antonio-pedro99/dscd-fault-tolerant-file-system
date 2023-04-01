from client import Client
import registry
import replica 
from threading import Thread
from time import sleep
import signal 
import os
from multiprocessing import Process
   

def start_registry():
    registry.main()

def start_replica():
    replica.main()
    

def main():
    num_replca=3
    process=[]
    try:
        registry = Process(target=start_registry)
        registry.start()
        process.append(registry)
        sleep(2)
        for i in range(num_replca):
            print(f'\n Replica No {i+1} \n')
            registry = Process(target=start_replica)
            registry.start()
            process.append(registry)
            sleep(1)
    except KeyboardInterrupt:
        sleep(1)
        for p in process:
            os.kill(p.pid, signal.SIGINT)
            sleep(1)
    # for p in process:
        

if __name__=='__main__':
    main()