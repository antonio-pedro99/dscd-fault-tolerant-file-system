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
        
        self.primary = response.address
        print(self.primary)  
        print('REPLICA REGISTERED WITH ADDRESS: ',self.address)
      

    def Read(self, request, context):
        file_uuid=request.uuid

        # the uuid is present in map and file is also present (not none)
        if (file_uuid in self.data_store_map.keys()) and self.data_store_map[file_uuid]!=None:
            status='SUCCESS'
            filename=self.data_store_map[file_uuid][0]
            timestamp=self.data_store_map[file_uuid][1]
            content = None
            try:
                #path = os.path.join(self.folder, f'{filename}')
                file_path = self.folder.joinpath(filename)
                file = open(file_path, 'r')
                content = file.read()
                file.close()
            except:
                status='FAIL'
            return message.ReadResponse(status=status, name=filename, 
                                        content=content, version=timestamp)         


    def Write(self, request: message.WriteRequest, context):
        self.write_lock.acquire()
        print(f'WRITE REQUEST FOR FILE {request.uuid}: UUID')
        response = message.WriteResponse()
        if self.is_primary:
           response = self.BroadcastWrite(request = request, context = context)
        else:
            print("I am not the primary, I need to send it to the PR")
            stub = servicer.ReplicaStub(grpc.insecure_channel(self.primary))
            response = stub.BroadcastWrite(message.WriteRequest(name = request.name, content = request.content, uuid = request.uuid))
        
        self.write_lock.release()
        return response


    def Delete(self, request, context):
        self.write_lock.acquire()
        print(f'DELETE REQUEST FOR FILE {request.uuid}: UUID')
        response = None
        if self.is_primary:
            response = self.BroadcastDelete(request=request, context=context)
        else:
            print("I am not the primary, I need to send it to the PR")
            stub = servicer.ReplicaStub(grpc.insecure_channel(self.primary))
            response = stub.BroadcastDelete(message.ReadDeleteRequest(uuid = request.uuid))
        self.write_lock.release()
        return response


    def BroadcastDelete(self, request, context):
        total_ack_received = -1
        print("RECEIVED FORWARD DELETE REQUEST")

        reason=None
        response = self.LocalDelete(request, context)
        print(response.response)
        if response.response==0:
            total_ack_received+=1
        else:
            reason=response.reason  
        print(response.response) 

        print("Voila, Now I will broadcast")
        # here is the loop
        for _rep in self.replicas:
            stub = servicer.ReplicaStub(grpc.insecure_channel(_rep.address))
            reply = stub.LocalDelete(message.ReadDeleteRequest(uuid = request.uuid))
            print(reply.response) # remove later
            if reply.response==0:
                total_ack_received+=1
            elif reply.response==1:
                reason=reply.reason
                break
        if total_ack_received == len(self.replicas):
            return response
        else:
            return message.Response(response='FAIL', reason=reason)
        


    # this is local delete
    # handle all the consitions here
    def LocalDelete(self, request, context):
        file_uuid=request.uuid
        self.write_lock.acquire()

        # file is in local map and present in the folder 
        if (file_uuid in self.data_store_map.keys()) and self.data_store_map[file_uuid]:
            status='SUCCESS'
            reason='SUCCESS'
            filename=self.data_store_map[file_uuid][0]
            # try:
            file_path = self.folder.joinpath(filename)
            os.remove(file_path.resolve())
            self.data_store_map[file_uuid]=tuple((None, ctime(os.path.getctime(file_path.resolve()))))
            # except:
            #     status='FAIL'
            #     reason='FAILED TO DELETE'
            self.write_lock.release()
            return message.Response(response=status, reason=reason)
        
        self.write_lock.release()
        

    # this is handing local write
    def BroadcastWrite(self, request, context):
        total_ack_received = -1
        print("RECEIVED FORWARD WRITE REQUEST")

        response = self.LocalWrite(request, context)
        print(response.status)
        if response.status==0:
            total_ack_received+=1

        print("Voila, Now I will broadcast")
        # here is the loop
        for _rep in self.replicas:
            stub = servicer.ReplicaStub(grpc.insecure_channel(_rep.address))
            reply = stub.LocalWrite(message.WriteRequest(name=request.name, uuid = request.uuid, content=request.content))
            # print(reply.status) # remove later
            if reply.status==0:
                total_ack_received+=1
            elif reply.status==1:
                break
        if total_ack_received == len(self.replicas):
            return response
        else:
            return message.WriteResponse(status='FAIL', uuid='Null', version='Null')
            

    # here all the local writes are handled 
    # check all the conditions for write
    def LocalWrite(self, request, context):
        file_path = self.folder.joinpath(f"{request.name}.txt")
        with open(file_path.resolve(), "w") as f:
                f.write(request.content)
                f.close()
                print(file_path.name)
                self.data_store_map[request.uuid] = tuple((file_path.name, ctime(os.path.getctime(file_path.resolve()))))
        status = "SUCCESS"
        return message.WriteResponse(status=status, uuid=request.uuid, version=self.data_store_map[request.uuid][1])

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
        servicer.add_ReplicaServicer_to_server(replica, server)
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