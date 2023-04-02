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
        print('REPLICA REGISTERED WITH ADDRESS: ',self.address)
      

    def Read(self, request, context):
        file_uuid=request.uuid
        status ='SUCCESS'
        detail ='Operation completed successfully'
        # the uuid is present in map and file is also present (not none)
        if (file_uuid in self.data_store_map.keys()) and self.data_store_map[file_uuid]!=None:
            filename=self.data_store_map[file_uuid][0]
            timestamp=self.data_store_map[file_uuid][1]
            content = None
            try:
                file_path = self.folder.joinpath(filename)
                with open(file_path, 'r') as f:
                    content = f.read()
                    f.close()
            except IOError as e:
                status ='FAIL'
                detail = "UNKNOWN"
            return message.ReadResponse(status=status, name=filename, content=content, details = detail)
        #file does not exists
        elif file_uuid not in self.data_store_map.keys():
            status = "FAIL"
            detail = "FILE DOES NOT EXIST"
            return message.ReadResponse(status=status, name=None, 
                                        content=None, version=None, details=detail)    
        elif file_uuid in self.data_store_map.keys():
            file_path = self.folder.joinpath(self.data_store_map[file_uuid][0])
            timestamp = self.data_store_map[file_uuid][1]
            if file_path.exists() == False:
                status = "FAIL"
                detail = "FILE ALREADY DELETED"
                return message.ReadResponse(status=status, name=None, 
                                        content=None, version=timestamp, details=detail) 


    def Write(self, request: message.WriteRequest, context):
        self.write_lock.acquire()
        print(f'WRITE REQUEST FOR FILE {request.uuid}: UUID')
        response = message.WriteResponse()
        if self.is_primary:
           response = self.BroadcastWrite(request = request, context = context)
        else:
            print(f"REDIRECTING WRITE TO PR[{self.primary}]")
            stub = servicer.ReplicaStub(grpc.insecure_channel(self.primary))
            response = stub.BroadcastWrite(message.WriteRequest(name = request.name, content = request.content, uuid = request.uuid))
        
        self.write_lock.release()
        return response


    def Delete(self, request, context):
        self.write_lock.acquire()
        print(f'DELETE REQUEST FOR FILE {request.uuid}: UUID at [REPLICA]{self.address}')
        response = None
        if self.is_primary:
            response = self.BroadcastDelete(request=request, context=context)
        else:
            print(f"REDIRECTING DELETE TO PR[{self.primary}] at [REPLICA]{self.address}'")
            stub = servicer.ReplicaStub(grpc.insecure_channel(self.primary))
            response = stub.BroadcastDelete(message.ReadDeleteRequest(uuid = request.uuid))
        self.write_lock.release()
        return response


    def BroadcastDelete(self, request, context):
        total_ack_received = -1
        print("RECEIVED FORWARDED DELETE REQUEST")

        reason=None
        response = self.LocalDelete(request, context)
        print(response.response)
        if response.response==0:
            total_ack_received+=1
        else:
            reason=response.reason

        #print(self.replicas)

        # here is the loop
        for _rep in self.replicas:
            print(f"BROADCASTING UPDATE TO [{_rep.address}]")
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
    # handle all the conditions here
    def LocalDelete(self, request:message.ReadDeleteRequest, context):
        file_uuid=request.uuid
        uuids = self.data_store_map.keys()


        if file_uuid not in uuids:
            status = 'FAIL'
            reason = 'FILE DOES NOT EXIST'
            return message.Response(response=status, reason=reason)

        fila_name = self.data_store_map[file_uuid][0]
        file_path = self.folder.joinpath(fila_name)


        status='SUCCESS'
        reason='SUCCESS'
        
        # file is in local map and present in the folder 
        if  file_path.exists() == True:
            # try:
            # os.remove(file_path.resolve())
            
            print(f'Removing {file_path.resolve()}')
            try:
                file_path.unlink(missing_ok=True)
            
                print(f'Address: {self.address}')
                self.data_store_map[file_uuid]=tuple((None, ctime(os.path.getctime(file_path.resolve()))))
            except FileNotFoundError as e:
                pass
            return message.Response(response=status, reason=reason)
        else:
            status = 'FAIL'
            reason = 'FILE ALREADY DELETED'
            return message.Response(response=status, reason=reason)

    # this is handing local write
    def BroadcastWrite(self, request, context):
        total_ack_received = -1
        print("RECEIVED FORWARD WRITE REQUEST")

        response = self.LocalWrite(request, context)
        print(response.status)
        if response.status==0:
            total_ack_received+=1

    
        # here is the loop
        error_reply=None
        for _rep in self.replicas:
            print(f"BROADCASTING UPDATE TO [{_rep.address}]")
            stub = servicer.ReplicaStub(grpc.insecure_channel(_rep.address))
            reply = stub.LocalWrite(message.WriteRequest(name=request.name, uuid = request.uuid, content=request.content))
            # print(reply.status) # remove later
            if reply.status==0:
                total_ack_received+=1
            elif reply.status==1:
                error_reply=reply
                break
        if total_ack_received == len(self.replicas):
            return response
        else:
            return error_reply
            

    # here all the local writes are handled 
    # check all the conditions for write
    def LocalWrite(self, request:message.WriteRequest, context):
        file_path = self.folder.joinpath(f"{request.name}.txt")
        file_uuid = request.uuid

        uuids = self.data_store_map.keys()
        #New File
        if file_uuid not in uuids and file_path.exists() == False:
            with open(file_path.resolve(), "w") as f:
                f.write(request.content)
                f.close()
                self.data_store_map[file_uuid] = tuple((file_path.name, ctime(os.path.getctime(file_path.resolve()))))
            status  = "SUCCESS"
            version = self.data_store_map[file_uuid][1]
           
            return message.WriteResponse(status=status, uuid=file_uuid, version=version, message = None)
        
        elif file_uuid not in uuids and file_path.exists() == True:
            status = "FAIL"
            status_message = "FILE WITH THE SAME NAME ALREADY EXISTS"
            print(status_message)
            return message.WriteResponse(status=status, uuid=None, version=None, message = status_message)
        elif file_uuid in uuids and file_path.exists() == True:
            with open(file_path.resolve(), "a") as f:
                f.write(request.content)
                f.close()
                self.data_store_map[file_uuid]  = tuple((file_path.name), ctime(os.path.getctime(file_path.resolve())))
            status = "SUCCESS"
            version = self.data_store_map[file_uuid][1]
            status_message = "File updated successfully"
            return message.WriteResponse(status=status, uuid=file_uuid, version=version, message = status_message)
        elif file_uuid in uuids and file_path.exists() == False:
            status = 'FAIL'
            status_message = "DELETED FILE CANNOT BE UPDATED"
            version = self.data_store_map[file_uuid][1]
            return message.WriteResponse(status=status, uuid=file_uuid, version=version, message = status_message)


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
        return

if __name__=='__main__':
    main() 