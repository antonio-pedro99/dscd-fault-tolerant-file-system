import grpc
import backup_protocol_pb2 as message
import backup_protocol_pb2_grpc as servicer
from port import get_new_port
import uuid
from concurrent import futures
from threading import Lock

class Replica(servicer.ReplicaServicer):

    def __init__(self):
        super().__init__()
        self.address='localhost:'+str(get_new_port())
        self.is_primary = False
        self.uuid=str(uuid.uuid1())
        self.registry_channel=grpc.insecure_channel('localhost:8888')
        self.replicas = []
        self.replicas_lock = Lock()

    def start(self):
        try:
            print('-----STARTING REPLICA------')
            self.SetupReplica()
            self.RegisterReplica()
        except KeyboardInterrupt:
            print('-----CLOSING REPLICA------')
            return

    def SetupReplica(self):
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=50))
        servicer.add_ReplicaServicer_to_server(Replica(),self.server)
        print("REPLICA STARTED")
        self.server.add_insecure_port(self.address)

    def RegisterReplica(self):
        register_replica_stub = servicer.RegistryServerStub(self.registry_channel)
        response = register_replica_stub.RegisterReplica(
            message.ServerMessage(uuid=self.uuid,address=self.address)
        )

        if response.address == 'EMPTY':
            self.is_primary = True

        print('REPLICA REGISTERED WITH ADDRESS: ',self.address)
        self.server.start()
        self.server.wait_for_termination()

    def Read(self, request, context):
        pass

    def Rrite(self, request, context):
        pass

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

def main():
    my_replica=Replica()
    my_replica.start()

if __name__=='__main__':
    main() 