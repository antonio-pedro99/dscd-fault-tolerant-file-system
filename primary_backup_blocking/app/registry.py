import grpc
import backup_protocol_pb2 as message
import backup_protocol_pb2_grpc as servicer
from concurrent import futures
from threading import Lock

class ReplicaRegistryService(servicer.RegistryServerServicer):

    def __init__(self) -> None:
        super().__init__()
        self.current_registered=0
        self.replica_list=[]
        self.replica_list_lock=Lock()
        self.primary_replica=None # we will be saving object of ServerMessage-> it will contain both uuid and address
        

    def start(self):
        try:
            print("STARTING REGISTRY")
            registry_replica = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
            servicer.add_RegistryServerServicer_to_server(ReplicaRegistryService(),registry_replica)
            print("REGISTRY STARTED")
            registry_replica.add_insecure_port('localhost:8888')
            registry_replica.start()
            registry_replica.wait_for_termination()
        except KeyboardInterrupt:
            print("------CLOSING REGISTRY------")
            return

    def RegisterReplica(self, request, context):
        self.replica_list_lock.acquire()
        print(f"JOIN REQUEST FROM REPLICA {request.address} [ADDRESS]")        
        replica=message.ServerMessage(uuid=request.uuid, address=request.address)
        self.replica_list.append(replica)
        # first replica
        if(len(self.replica_list)==1):
            self.primary_replica=replica
            self.replica_list_lock.release()
            return message.ServerMessage(uuid='EMPTY', address='EMPTY')
        # not first
        else:
            #Notify primary
            replica_stub = servicer.ReplicaStub(grpc.insecure_channel(self.primary_replica.address))

            replica_stub.NotifyPrimary(message.ServerMessage(uuid=replica.uuid, address=replica.address))
            
            self.replica_list_lock.release()
            return message.ServerMessage(uuid=self.primary_replica.uuid ,
                                         address=self.primary_replica.address)

    
    def GetReplicas(self, request, context):
        self.replica_list_lock.acquire()
        print(f"replica LIST REQUEST FROM {request.id} [ADDRESS]")
        all_replicas=message.replicaList()
        all_replicas.replicaList.extend(list(self.replica_list.values()))
        self.replica_list_lock.release()
        return all_replicas
    

def main():
    my_registry=ReplicaRegistryService()
    my_registry.start()


if __name__=='__main__':
    main()