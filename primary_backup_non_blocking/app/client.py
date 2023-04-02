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
    
    def write(self, replica:message.ServerMessage, name=None, content=None , file_uuid=None):
        
        replica_stub = servicer.ReplicaStub(channel = grpc.insecure_channel(replica.address) )
        if file_uuid == None:
            file_uuid = str(uuid.uuid4())
        request = message.WriteRequest(name=name, uuid=file_uuid, content=content)
        response = replica_stub.Write(request)
        print(response)
      


    def read(self,file_uuid,replica:message.ServerMessage):
        replica_stub = servicer.ReplicaStub(channel = grpc.insecure_channel(replica.address) )
        request = message.ReadDeleteRequest(uuid=file_uuid)
        response = replica_stub.Read(request)
        print(response)

    def delete(self, file_uuid, replica:message.ServerMessage):
        replica_stub = servicer.ReplicaStub(channel = grpc.insecure_channel(replica.address) )
        request = message.ReadDeleteRequest(uuid=file_uuid)
        response = replica_stub.Delete(request)
        print(response)

    def read_all(self, file_uuid):
        if file_uuid!='': # read uuid from every replica
            request=message.ReadDeleteRequest(uuid=file_uuid)
        else:  # real all data from all the replicas
            request=message.ReadDeleteRequest(uuid='Null')
        read_replicas=list(map(lambda x: x.address, self.replicas))
        print(f'{"="*10} ALL DATA {"="*10}')
        for replica in read_replicas:
            print(f'From Address: {replica}')
            channel = grpc.insecure_channel(str(replica))
            read_stub = servicer.ReplicaStub(channel)
            response = read_stub.GetAllData(request)
            for data in response.readResponseList:
                print(data)
        print(f'{"="*30}')


def show_menu(client:Client):
    while True:
   
        replica = random.choice(client.replicas)

        print("\n---------MENU--------\n1. Write")
        print("2. Read\n3. Delete\n4. Real All\n5. Exit\n")
        try:
            choice=int(input('Choose one option: '))
            if(choice==1):
                file_uuid = input("Enter the UUID[Optional]: ")
                name = input("Enter the file name: ")
                content = input("Enter the file content: ")
                if file_uuid != "":
                    client.write(replica=replica, name = name, content = content, file_uuid = file_uuid)
                else:
                    client.write(replica=replica, name = name, content = content)
            elif(choice==2):
                file_uuid=input("Enter the uuid of file: ")
                client.read(file_uuid = file_uuid, replica=replica)
            elif(choice==3):
                file_uuid=input("Enter the uuid of file: ")
                client.delete(file_uuid = file_uuid, replica=replica)
            elif(choice==4):
                file_uuid=input("Enter the UUID[Optional]: ")
                client.read_all(file_uuid)
            elif(choice==5):
                print("EXITING")
                return
        except ValueError:
            print("[ERROR] Incorrect Input")

def main():
    my_client=Client()
    show_menu(my_client)

if __name__=='__main__':
    main()