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
        self.Read(x) # create new file with existing name
        # self.Write('ads','Alien', x) # update existing file
        # 
        pass

    def Read(self, file_uuid):
        read_replicas=self.get_replicas('READ')
        request=message.ReadDeleteRequest(uuid=file_uuid)
        status = []
        all_name = []
        all_version = []
        all_content=[]
        for replica in read_replicas:
            channel = grpc.insecure_channel(str(replica))
            read_stub = servicer.ReplicaStub(channel)
            response = read_stub.Read(request)
            status.append(response.status)
            all_name.append(response.name)
            all_content.append(response.content)
            all_version.append(response.version)

        error = False
        reason = None
        max_version = None #datetime.datetime.strptime(all_version[0], "%Y-%m-%d %H:%M:%S.%f")
        index=None
        if 'FILE ALREADY DELETED' in all_content:
            error = True
            reason = 'REASON: FILE ALREADY DELETED'
        elif 0 not in status:
            error = True
            reason = 'REASON: FILE DOES NOT EXIST'
        else:
            for i in range(len(all_version)):
                if(status[i]==0):
                    current = datetime.datetime.strptime(all_version[i], "%Y-%m-%d %H:%M:%S.%f")
                    if max_version == None or current > max_version:
                        max_version = current
                        index = i
        if error:
            print('***** READ FAILED *****')
            print(reason)
        else:
            print('***** READ SUCCESS *****')
            print(f'name: {all_name[index]}')
            print(f'content: {all_content[i]}')
            print(f'version: {all_version[i]}')
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
        # returning for testing purpose
        return file_uuid
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