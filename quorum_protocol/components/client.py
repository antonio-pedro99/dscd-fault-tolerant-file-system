import grpc
import backup_protocol_pb2 as message
import backup_protocol_pb2_grpc as servicer
from port import get_new_port
import uuid
from concurrent import futures
from threading import Lock


class Client:

    def __init__(self) -> None:
        pass

    def Read(self):
        pass

    def Write(self, name, text, uuid=None):
        write_replicas=self.get_replicas('WRITE')
        if(uuid==None):
            uuid=str(uuid.uuid1())
        request=message.WriteRequest(name=name, content=text, uuid=uuid)
        for replica in write_replicas:
            channel=grpc.insecure_channel(replica)
            write_stub = servicer.ReplicaStub(channel)
            response = write_stub.Write(request)
            
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