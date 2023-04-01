import grpc
import backup_protocol_pb2 as message
import backup_protocol_pb2_grpc as servicer
from port import get_new_port
import uuid
from concurrent import futures
from threading import Lock
import os
import shutil
import pandas as pd
from time import sleep

class Replica(servicer.ReplicaServicer):

    def __init__(self, address):
        super().__init__()
        self.MAXCLIENTS=10
        self.uuid=str(uuid.uuid1())
        # making a unified lock for while folder
        # we can also have locks for individual file
        self.files={}
        self.folder_lock=Lock()
        self.address=address
        port=tuple(self.address.split(':'))[1]
        self.folder=os.path.join(str(os.getcwd()),"database",port)
        self.registry_channel=grpc.insecure_channel('localhost:50001')
        pass

    def start(self):
        self.RegisterReplica()
        self.CreateDirectory()

    def stop(self):
        shutil.rmtree(self.folder) # deleting the directory

    def CreateDirectory(self):
        try:
            os.makedirs(self.folder, 0o777 )
        except OSError as error: 
            print("[ERROR] Creating replica folder")
            print(error)


    def SetupReplica(self):
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=self.MAXCLIENTS))
        servicer.add_ReplicaServicer_to_server(Replica(),self.server)
        print("REGISTRY STARTED")
        self.server.add_insecure_port(self.address)

    def RegisterReplica(self):
        register_replica_stub = servicer.RegistryServerStub(self.registry_channel)
        response = register_replica_stub.RegisterReplica(
            message.ServerMessage(uuid=self.uuid,address=self.address)
        )
        print('REPLICA REGISTERED WITH ADDRESS: ',self.address)


    def Read(self, request, context):
        pass

    def Write(self, request, context):
        # handling the write request received from client
        filename=request.name
        content=request.content
        file_uuid=request.uuid
        time = pd.Timestamp('now', tz='Asia/Kolkata').time()
        # self.folder_lock.acquire()

        # new file is been written
        if file_uuid not in self.files.keys():
            response = self.write_new_file(filename, content, file_uuid, time)
            # self.folder_lock.release()
            return response

    def write_new_file(self, filename, content, file_uuid, timestamp):
        status='SUCCESS'
        # try:
        print("came here 2")
        path = os.path.join(self.folder, f'{filename}.txt')
        print(path)
        file = open(path, 'w+')
        file.write(content)
        #file.close()
        # self.files[file_uuid]=tuple(filename,timestamp)
        # except:
        #     status='FAIL'
        print("came here")
        return message.WriteResponse(status=status, uuid=file_uuid, version=str("timestamp"))


    def Delete(self, request, context):
        pass



def main(address=None):
    if(address==None):
        address='localhost:'+str(get_new_port())
    my_replica=Replica(address=address)
    MAXCLIENTS=10
    try:
        print('-----STARTING REPLICA------')
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=MAXCLIENTS))
        server.add_insecure_port(address)
        servicer.add_ReplicaServicer_to_server(Replica(address),server)
        print("REGISTRY STARTED")
        my_replica.start()
        server.start()
        server.wait_for_termination()
    except KeyboardInterrupt:
        print('-----CLOSING REPLICA------')
        my_replica.stop()
        return
    return


if __name__=='__main__':
    main()