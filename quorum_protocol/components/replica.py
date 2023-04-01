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

    def __init__(self):
        super().__init__()
        self.address='localhost:'+str(get_new_port())
        self.MAXCLIENTS=10
        self.uuid=str(uuid.uuid1())
        # making a unified lock for while folder
        # we can also have locks for individual file
        self.files={}
        self.folder_lock=Lock()
        self.registry_channel=grpc.insecure_channel('localhost:50001')
        self.folder=os.path.join(str(os.getcwd()),"database",self.uuid)
        pass

    def start(self):
        try:
            print('-----STARTING REPLICA------')
            self.SetupReplica()
            self.CreateDirectory()
            # self.write_new_file( "hello", "plat", "cskdj ksf c", "hello therer")
            self.RegisterReplica()
        except KeyboardInterrupt:
            print('-----CLOSING REPLICA------')
            shutil.rmtree(self.folder) # deleting the directory
            return

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
        self.server.start()
        self.server.wait_for_termination()


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
        path = os.path.normpath(path)
        print(path)
        f = open(path.removesuffix("\\"), 'w')
        #file_path=os.path.join(self.folder,f"{filename}.txt")
        #print('\n',file_path)
        #f = open(f"{self.folder}/t.txt", 'w+')
        #file.write(content)
        #file.close()
        # self.files[file_uuid]=tuple(filename,timestamp)
        # except:
        #     status='FAIL'
        print("came here")
        return message.WriteResponse(status=status, uuid=file_uuid, version=str(timestamp))


    def Delete(self, request, context):
        pass



def main():
    my_replica=Replica()
    my_replica.start()
    return


if __name__=='__main__':
    main()