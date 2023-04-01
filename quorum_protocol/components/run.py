from client import Client
from registry_server import RegistryServer
from replica import Replica
from threading import Thread
from time import sleep
import signal 
import os
from multiprocessing import Process
   

def start_registry():
    my_registry=RegistryServer()
    my_registry.start(1,1,1)
    

def main():
    process = Process(target=start_registry)
    process.start()
    print(process.pid)
    sleep(5)
    
    # os.kill(process.pid, signal.SIGINT)
    process.join()

if __name__=='__main__':
    main()