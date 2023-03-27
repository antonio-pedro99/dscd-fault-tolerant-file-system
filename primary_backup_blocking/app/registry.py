import grpc
import backup_protocol_pb2 as message
import backup_protocol_pb2_grpc as servicer
from concurrent import futures
from threading import Lock

class ReplicaRegistryService:

    def __init__(self) -> None:
        super().__init__()
        self.current_registered=0
        self.replica_list={}
        self.replica_list_lock=Lock()
        self.primary_replica_address = None
        self.primary_replica_uuid = None
    
    def start(self):
        try:
            print("STARTING REGISTRY")
            registry_replica = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
            servicer.add_ReplicaServicer_to_server(ReplicaRegistryService(),registry_replica)
            print("REGISTRY STARTED")
            registry_replica.add_insecure_port('localhost:8888')
            registry_replica.start()
            registry_replica.wait_for_termination()
        except KeyboardInterrupt:
            print("------CLOSING REGISTRY------")
            return

    def register_replica(self, request, context):
        self.replica_list_lock.acquire()
        print(f"JOIN REQUEST FROM {request.address} [ADDRESS]")
    
        self.replica_list[request.name]=message.replicaMessage(name=request.name,address=request.address)
        self.current_registered+=1
        self.replica_list_lock.release()
        return message.Result()
    
    def get_replica_list(self, request, context):
        self.replica_list_lock.acquire()
        print(f"replica LIST REQUEST FROM {request.id} [ADDRESS]")
        all_replicas=message.replicaList()
        all_replicas.replicaList.extend(list(self.replica_list.values()))
        self.replica_list_lock.release()
        return all_replicas
    
    def read(self):
        pass

def main():
    my_registry=ReplicaRegistryService()
    my_registry.start()


if __name__=='__main__':
    main()