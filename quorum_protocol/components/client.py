import grpc
import backup_protocol_pb2 as message
import backup_protocol_pb2_grpc as servicer
from port import get_new_port
import uuid
from concurrent import futures
from threading import Lock
import datetime


class Client:

    def __init__(self) -> None:
        self.registry_channel=grpc.insecure_channel('localhost:50001')
        pass

    def start(self):
        x = self.Write('ads','plant') # create new file
        self.Write('ads','Hello') # create new file with existing name
        # self.Write('ads','Alien', x) # update existing file
        # 
        pass

    def Read(self):
        pass

    def Write(self, name, text, file_uuid=None):
        write_replicas=self.get_replicas('WRITE')
        # print(write_replicas)
        if(file_uuid==None): # it is a new file that is been written 
            file_uuid=str(uuid.uuid1())
        request=message.WriteRequest(name=name, content=text, uuid=file_uuid)
        status = []
        all_uuid = []
        version = []
        for replica in write_replicas:
            channel=grpc.insecure_channel(str(replica))
            write_stub = servicer.ReplicaStub(channel)
            response = write_stub.Write(request)
            status.append(response.status)
            all_uuid.append(response.uuid)
            version.append(response.version)
            # print(response)
            # date_object = datetime.datetime.strptime(response.version, "%Y-%m-%d %H:%M:%S.%f")
            # print(type(date_object))
        error = False
        reason = None
        max_version =datetime.datetime.strptime(version[0], "%Y-%m-%d %H:%M:%S.%f")
        # print(status)
        for i in range(len(status)):
            if status[i]==1:
                error = True
                reason = f'REASON: {all_uuid[i]}'
            if (datetime.datetime.strptime(version[i], "%Y-%m-%d %H:%M:%S.%f") > max_version):
                max_version = datetime.datetime.strptime(version[i], "%Y-%m-%d %H:%M:%S.%f")
        if error:
            print("***** WRITE FAILED *****")
            print(reason)
        else:
            print("***** WRITE SUCCESS *****")
            print(f'uuid: {file_uuid}')
            print(f'version: {max_version}')
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