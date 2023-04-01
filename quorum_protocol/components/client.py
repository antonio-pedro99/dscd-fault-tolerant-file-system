import grpc
import backup_protocol_pb2 as message
import backup_protocol_pb2_grpc as servicer
from port import get_new_port
import uuid
from concurrent import futures
from threading import Lock


class Client:

    def __init__(self) -> None:
        self.registry_channel=grpc.insecure_channel('localhost:50001')
        pass

    def start(self):
        self.Write('ads','adsaad')

    def Read(self):
        pass

    def Write(self, name, text, file_uuid=None):
        write_replicas=self.get_replicas('WRITE')
        print(write_replicas)
        if(file_uuid==None): # it is a new file that is been written 
            file_uuid=str(uuid.uuid1())
        request=message.WriteRequest(name=name, content=text, uuid=file_uuid)
        for replica in write_replicas:
            print('sending in', replica)
            channel=grpc.insecure_channel(str(replica))
            write_stub = servicer.ReplicaStub(channel)
            response = write_stub.Write(request)
            print(response)

        pass

    def Delete(self):
        pass

    # READ, WRITE and DELETE
    def get_replicas(self, type):
        get_server_lst_stub = servicer.RegistryServerStub(self.registry_channel)
        response = get_server_lst_stub.GetReplicas(
            message.RequestType(type=type)
        )
        return list([msg.address for msg in response.serverList])
    


def main():
    my_client=Client()
    my_client.start()
    return


if __name__=='__main__':
    main()