import grpc
import backup_protocol_pb2 as message
import backup_protocol_pb2_grpc as servicer
import uuid
import datetime
from time import sleep
from google.protobuf import empty_pb2
import random

class Client:

    def __init__(self) -> None:
        self.registry_channel=grpc.insecure_channel('localhost:50001')

    def run_test(self):
        x = self.Write('file1','THIS IS FILE 1') # create new file
        sleep(2)
        self.Read(x) # create new file with existing name
        self.read_all()
        sleep(2)
        y = self.Write('file2','THIS IS FILE 2') # update existing file
        self.read_all()
        sleep(2)
        self.Read(y)
        self.read_all()
        sleep(2)
        self.Delete(x)
        self.read_all()
        sleep(2)
        self.Read(x)
        self.read_all()
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
        # print(status)
        # print(all_name)
        # print(all_content)
        # print(all_version)
        if 'FILE ALREADY DELETED' in all_name:
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
            print(f'content: {all_content[index]}')
            print(f'version: {all_version[index]}')
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
          
        error = False
        reason = None
        max_version =datetime.datetime.strptime(version[0], "%Y-%m-%d %H:%M:%S.%f")
     
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

    def Delete(self, file_uuid):
        delete_replicas=self.get_replicas('WRITE')
        request=message.ReadDeleteRequest(uuid=file_uuid)
        status = []
        all_reason= []
        for replica in delete_replicas:
            channel = grpc.insecure_channel(str(replica))
            read_stub = servicer.ReplicaStub(channel)
            response = read_stub.Delete(request)
            status.append(response.response)
            all_reason.append(response.reason)

        error=False
        reason=None
        if 'FILE ALREADY DELETED' in all_reason:
            error=True
            reason='FILE ALREADY DELETED'
        elif 1 in status:
            error=True
            reason='FAILED TO DELETE'
        else:
            error=False
        
        if error:
            print("***** DELETE FAILED *****")
            print(f'REASON: {reason}')
        else:
            print("***** DELETE SUCCESS *****")
            print(f'uuid: {file_uuid}')


    # READ, WRITE and DELETE
    def get_replicas(self, type):
        get_server_lst_stub = servicer.RegistryServerStub(self.registry_channel)
        response = get_server_lst_stub.GetReplicas(
            message.RequestType(type=type)
        )
        return list([msg.address for msg in response.serverList])
    

    def read_all(self, file_uuid):
        if file_uuid!='': # read uuid from every replica
            request=message.ReadDeleteRequest(uuid=file_uuid)
        else:  # real all data from all the replicas
            request=message.ReadDeleteRequest(uuid='Null')
        read_replicas=self.get_replicas('ALL')
        print(f'{"="*10} ALL DATA {"="*10}')
        for replica in read_replicas:
            print(f'From Address: {replica}')
            channel = grpc.insecure_channel(str(replica))
            read_stub = servicer.ReplicaStub(channel)
            response = read_stub.GetAllData(request)
            for data in response.readResponse:
                print(data)
        print(f'{"="*30}')


def show_menu(client:Client):
    while True:
   
        #lient.get_replicas("ALL")

        print("\n---------MENU--------\n1. Write")
        print("2. Read\n3. Delete\n4. Read All\n5. Exit\n")
        try:
            choice=int(input('Choose one option: '))
            if(choice==1):
                file_uuid = input("Enter the UUID[Optional]: ")
                name = input("Enter the file name: ")
                content = input("Enter the file content: ")
                if file_uuid != "":
                    client.Write(name = name, text = content, file_uuid = file_uuid)
                else:
                    client.Write(name = name, text = content)
            elif(choice==2):
                file_uuid=input("Enter the uuid of file: ")
                client.Read(file_uuid = file_uuid)
            elif(choice==3):
                file_uuid=input("Enter the uuid of file: ")
                client.Delete(file_uuid = file_uuid)
            elif(choice==4):
                file_uuid=input("Enter the UUID[Optional]: ")
                client.read_all(file_uuid=file_uuid)
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