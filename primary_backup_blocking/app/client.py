import grpc
import backup_protocol_pb2 as message
import backup_protocol_pb2_grpc as servicer
from concurrent import futures
from google.protobuf import empty_pb2
import uuid

class Client:
    def __init__(self) -> None:
        # TODO document why this method is empty
        self.registry_channel=grpc.insecure_channel('localhost:8888')
        self.registry_stub = servicer.RegistryServerStub(self.registry_channel)


    def get_replicas(self):
        print('----------------\nLIST OF AVAILABLE REPLICAS')
        response = self.registry_stub.GetReplicas(empty_pb2.Empty())
    
        for replica in response.serverList:
            print(f'{replica.uuid} {replica.address}')
        
    
    def write(self):
        # TODO document why this method is empty
        pass

    def read(self):
        # TODO document why this method is empty
        pass

    def delete(self):
        # TODO document why this method is empty
        pass


def main():
    my_client=Client()
    my_client.get_replicas()
    #show_menu(my_client)

if __name__=='__main__':
    main()