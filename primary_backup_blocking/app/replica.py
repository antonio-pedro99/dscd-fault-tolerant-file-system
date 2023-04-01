import grpc
import backup_protocol_pb2 as message
import backup_protocol_pb2_grpc as servicer
from port import get_new_port
from google.protobuf import empty_pb2
import os
import shutil
from time import ctime
import uuid
from concurrent import futures
from threading import Lock
from pathlib import Path

ROOT_DIR = Path.cwd()

class Replica(servicer.ReplicaServicer):

    def __init__(self, address:str):
        super().__init__()

        self.is_primary = False
        self.uuid=str(uuid.uuid4())
        self.address = address
        self.folder = ROOT_DIR.joinpath("db", self.address.split(":")[-1])
        self.registry_channel=grpc.insecure_channel('localhost:8888')
        self.replicas = []
        self.data_store_map = {}
        self.replicas_lock = Lock()
        self.write_lock = Lock()


    def start(self):
        self.__create_dir__()
        self.RegisterReplica()
    
    def stop(self):
        shutil.rmtree(self.folder.resolve())

    def __create_dir__(self):
        try:
            self.folder.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            print("[ERROR] Creating replica folder")
            print(e)


    def RegisterReplica(self):
        register_replica_stub = servicer.RegistryServerStub(self.registry_channel)
        response = register_replica_stub.RegisterReplica(
            message.ServerMessage(uuid=self.uuid,address=self.address)
        )

        if response.address == 'EMPTY':
            self.is_primary = True

        print('REPLICA REGISTERED WITH ADDRESS: ',self.address)

    def Read(self, request, context):
        pass

    def Write(self, request: message.WriteRequest, context):
        self.write_lock.acquire()
        print(f'WRITE REQUEST FOR FILE {request.uuid}: UUID')
        
       
        status = "SUCCESS"
        file_name = f"{request.name}.txt"
        file_path = self.folder.joinpath(file_name)
        if request.uuid not in self.data_store_map.keys() and file_path.exists() == False:
            #new file
            with open(file_path.resolve(), "w") as f:
                f.write(request.content)
                #add the file uuid as the key and the version as the value
                f.close()
                self.data_store_map[request.uuid] = ctime(os.path.getctime(file_path.resolve()))
                
            status = "SUCCESS"
        
        response = message.WriteResponse(status=status, uuid=request.uuid, version=self.data_store_map[request.uuid])
        
        self.write_lock.release()
        return response
    
    def Delete(self, request, context):
        pass

    def HandleWrite(self, request, context):
        pass

    def NotifyPrimary(self, request, context):
        self.replicas_lock.acquire()
        new_replica = message.ServerMessage(uuid=request.uuid, address=request.address)
        self.replicas.append(new_replica)
        print(f"NEW REPLICA {request.address} [ADDRESS] JOINED")  
        self.replicas_lock.release()
        return empty_pb2.Empty()

def main():
    address = 'localhost:'+str(get_new_port())
    replica = Replica(address = address)

    try:
        print('-----STARTING REPLICA------')
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=50))
        server.add_insecure_port(address = address)
        servicer.add_ReplicaServicer_to_server(Replica(address = address),server)
        print("REGISTRY STARTED")

        replica.start()
        server.start()
        server.wait_for_termination()
        
    except KeyboardInterrupt:
        print('-----CLOSING REPLICA------')
        replica.stop()
        return;

if __name__=='__main__':
    main() 