import grpc
import backup_protocol_pb2 as message
import backup_protocol_pb2_grpc as servicer
from concurrent import futures
from threading import Lock
import random

class RegistryServerService(servicer.RegistryServerServicer):

    def __init__(self) -> None:
        super().__init__()
        self.current_registered=0
        self.replica_list={}
        self.replica_list_lock=Lock()
    
    def start(self, N, N_w,  N_r):
        self.num_replica=N
        self.N_r=N_r
        self.N_w=N_w
        try:
            print("STARTING REGISTRY")
            registry_server = grpc.server(futures.ThreadPoolExecutor(max_workers=self.num_replica))
            servicer.add_RegistryServerServicer_to_server(RegistryServerService(),registry_server)
            print("REGISTRY STARTED")
            registry_server.add_insecure_port('localhost:50001')
            registry_server.start()
            registry_server.wait_for_termination()
        except KeyboardInterrupt:
            print("------CLOSING REGISTRY------")
            return


    def RegisterReplica(self, request, context):
        self.replica_list_lock.acquire()
        print(f"JOIN REQUEST FROM REPLICA {request.address} [ADDRESS]")
        self.replica_list[request.uuid]=message.ServerMessage(uuid=request.uuid,address=request.address)
        self.current_registered+=1
        self.replica_list_lock.release()
        return message.ServerMessage(uuid=None,address=None)

    def GetReplicas(self, request, context):
        self.replica_list_lock.acquire()
        request_type=str(request.type)
        num_replica_to_send=0
        if(request_type=="READ"):
            num_replica_to_send=self.N_r
        elif(request_type=="WRITE" or request_type=="DELETE"):
            num_replica_to_send=self.N_w
        else: # none of the 
            num_replica_to_send=self.num_replica
        uids_to_send=list(random.sample(self.replica_list.keys(),num_replica_to_send))
        replicas_to_send=message.ServerListResponse()
        for key in uids_to_send:
            replicas_to_send.serverList.append(self.replica_list[key])
        self.replica_list_lock.release()
        return replicas_to_send
    
    
    def GetServerList(self, request, context):
        self.server_list_lock.acquire()
        print(f"SERVER LIST REQUEST FROM {request.id} [ADDRESS]")
        all_servers=message.ServerList()
        all_servers.serverList.extend(list(self.server_list.values()))
        self.server_list_lock.release()
        return all_servers

def main():
    while(True):
        try:
            # asking imputs for the N_r, N_w and N
            N = int(input("Enter number of replicas(N): "))
            N_w = int(input("Enter number of write replicas(N_w): "))
            N_r = int(input("Enter number of read replicas(N_r): "))

            # check for constrains
            if N_r<=N and N_w<=N and N_w>(N//2) and (N_r+N_w)>N:
                break
            else:
                print("[ERROR] Condition does not satisfy. Try again")
        except:
            print("[ERROR] Invalid input please try again.")
    #initalising the server 
    my_registry=RegistryServerService()
    my_registry.start(N, N_w, N_r)


if __name__=='__main__':
    main()