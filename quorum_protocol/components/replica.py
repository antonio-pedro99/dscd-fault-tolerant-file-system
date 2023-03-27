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
        self.MAXCLIENTS=10
        self.uuid=str(uuid.uuid1())
        # self.CLIENTELE=[]
        # self.article_list=[]
        # self.client_lock=Lock()
        # self.article_lock=Lock()
        self.registry_channel=grpc.insecure_channel('localhost:50001')
        pass

    def start(self):
        try:
            print('-----STARTING REPLICA------')
            self.SetupReplica()
            self.RegisterReplica()
        except KeyboardInterrupt:
            print('-----CLOSING REPLICA------')
            return

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
        print(self.get_replicas('READ'))
        self.server.start()
        self.server.wait_for_termination()

    def get_replicas(self, type):
        get_server_lst_stub = servicer.RegistryServerStub(self.registry_channel)
        response = get_server_lst_stub.GetReplicas(
            message.RequestType(type=type)
        )
        return list([msg.address for msg in response.serverList])

    def Read(self, request, context):
        print(self.get_replicas("READ"))
        pass

    def Write(self, request, context):
        pass

    def Delete(self, request, context):
        pass

    def HandleWrite(self, request, context):
        pass


def main():
    my_replica=Replica()
    my_replica.start()
    return


if __name__=='__main__':
    main()