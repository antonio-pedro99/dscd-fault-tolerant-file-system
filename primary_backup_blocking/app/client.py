import grpc
import backup_protocol_pb2 as message
import backup_protocol_pb2_grpc as servicer
from concurrent import futures
from google.protobuf import empty_pb2
import uuid
from typing import List
import random

class Client:
    def __init__(self) -> None:
        # TODO document why this method is empty
        self.registry_channel=grpc.insecure_channel('localhost:8888')
        self.registry_stub = servicer.RegistryServerStub(self.registry_channel)
        self.replicas = self.__get_replicas__()


    def __get_replicas__(self)-> List[message.ServerMessage]:
        print('----------------\nLIST OF AVAILABLE REPLICAS')
        try:
            response = self.registry_stub.GetReplicas(empty_pb2.Empty())
    
            return response.serverList
        except Exception as e:
            return e
    
    def write(self, replica:message.ServerMessage):
        
        replica_stub = servicer.ReplicaStub(channel = grpc.insecure_channel(replica.address) )
        file_uuid = str(uuid.uuid4())
        name = input("Enter the file name: ")
        content = input("Enter the file content: ")
        request = message.WriteRequest(name=name, uuid=file_uuid, content=content)
        response = replica_stub.Write(request)
        print(response)
      


    def read(self,replica:message.ServerMessage):
        # TODO document why this method is empty
        pass

    def delete(self):
        # TODO document why this method is empty
        pass


def show_menu(client:Client):
    while True:
   
        replica = random.choice(client.replicas)

        print("\n---------MENU--------\n1. Write")
        print("2. Read\n3. Exit\n")
        try:
            choice=int(input('Choose one option: '))
            if(choice==1):
                client.write(replica=replica)
            elif(choice==2):
                client.read(replica=replica)
            elif(choice==3):
                print("EXITING")
                return
        except ValueError:
            print("[ERROR] Incorrect Input")

def main():
    my_client=Client()
    show_menu(my_client)

if __name__=='__main__':
    main()