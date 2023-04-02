import grpc
import backup_protocol_pb2 as message
import backup_protocol_pb2_grpc as servicer
from port import get_new_port
import uuid
from concurrent import futures
from threading import Lock
import os
import shutil
import pandas as pd
from time import sleep

class Replica(servicer.ReplicaServicer):

    def __init__(self, address):
        super().__init__()
        self.MAXCLIENTS=10
        self.uuid=str(uuid.uuid1())
        # making a unified lock for while folder
        # we can also have locks for individual file
        self.files={}
        self.folder_lock=Lock()
        self.address=address
        port=tuple(self.address.split(':'))[1]
        self.folder=os.path.join(str(os.getcwd()),"database",port)
        self.registry_channel=grpc.insecure_channel('localhost:50001')
        pass

    def start(self):
        self.RegisterReplica()
        self.CreateDirectory()

    def stop(self):
        shutil.rmtree(self.folder) # deleting the directory

    def CreateDirectory(self):
        try:
            os.makedirs(self.folder, 0o777 )
        except OSError as error: 
            print("[ERROR] Creating replica folder")
            print(error)


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


    def Write(self, request, context):
        # handling the write request received from client
        filename=request.name
        content=request.content
        file_uuid=request.uuid
        time = tuple(str(pd.Timestamp('now', tz='Asia/Kolkata').to_pydatetime()).split('+'))[0]
        self.folder_lock.acquire()

        # try:

        # deplicate file name is given
        all_filenames=list(map(lambda x: x[0] , self.files.values()))
        if (file_uuid not in self.files.keys()) and (filename in all_filenames):
            self.folder_lock.release()
            return message.WriteResponse(status='FAIL', 
                                         uuid='FILE WITH THE SAME NAME ALREADY EXISTS', 
                                         version=str(time))
        
        # new file is been written
        if file_uuid not in self.files.keys():
            response = self.write_file(filename, content, file_uuid, time)
            self.folder_lock.release()
            return response
            
        # updating the existing file
        if (file_uuid in self.files.keys()) and (filename in all_filenames):
            response = self.write_file(filename, content, file_uuid, time)
            self.folder_lock.release()
            return response
    
        # trying to update deleted file
        if (file_uuid in self.files.keys()) and (filename not in all_filenames):
            self.folder_lock.release()
            return message.WriteResponse(status='FAIL', 
                                         uuid='DELETED FILE CANNOT BE UPDATED', 
                                         version=str(time))

        # except:
        #     print('[ERROR] in Writing')


    def write_file(self, filename, content, file_uuid, timestamp):
        status='SUCCESS'
        try:
            path = os.path.join(self.folder, f'{filename}.txt')
            # print(path)
            file = open(path, 'w+')
            file.write(content)
            file.close()
            self.files[file_uuid]=tuple((filename,timestamp))
        except:
            status='FAIL'
        return message.WriteResponse(status=status, uuid=file_uuid, version=str(timestamp))

    def Read(self, request, context):
        file_uuid=request.uuid
        self.folder_lock.acquire()

        all_filenames=list(map(lambda x: x[0] , self.files.values()))
        # read the file that is not present
        if file_uuid not in self.files.keys():
            self.folder_lock.release()
            return message.ReadResponse(status='FAIL',name='FILE DOES NOT EXIST',
                                        content='Null',version='Null')
        
        # read an existing file
        if (file_uuid in self.files.keys()) and (self.files[file_uuid][0]!=None):
            response=self.read_avalable_file(file_uuid)
            self.folder_lock.release()
            return response

        # trying to read a file that is deleted
        if (file_uuid in self.files.keys()) and (self.files[file_uuid][0]==None):
            self.folder_lock.release()
            return message.ReadResponse(status='FAIL',name='FILE ALREADY DELETED',
                                        content='Null',version=self.files[file_uuid][1])
        pass


    def read_avalable_file(self,file_uuid):
        status='SUCCESS'
        filename=self.files[file_uuid][0]
        timestamp=self.files[file_uuid][1]
        try:
            path = os.path.join(self.folder, f'{filename}.txt')
            # print(path)
            file = open(path, 'r')
            content = file.read()
            file.close()
        except:
            status='FAIL'
        return message.ReadResponse(status=status, name=filename, 
                                    content=content, version=str(timestamp))


    def Delete(self, request, context):
        file_uuid=request.uuid
        self.folder_lock.acquire()
        timestamp = tuple(str(pd.Timestamp('now', tz='Asia/Kolkata').to_pydatetime()).split('+'))[0]

        # file is not present in the in memory map
        if file_uuid not in self.files.keys():
            self.files[file_uuid]=tuple((None, timestamp))
            self.folder_lock.release()
            return message.Response(response='SUCCESS', reason='SUCCESS')

        # file exist in the filesystem
        if (file_uuid in self.files.keys()) and (self.files[file_uuid][0]!=None):
            response = self.delete_available_file(file_uuid,timestamp)
            self.folder_lock.release()
            return response
        
        if (file_uuid in self.files.keys()) and (self.files[file_uuid][0]==None):
            self.folder_lock.release()
            return message.Response(response='FAIL', reason='FILE ALREADY DELETED')

        pass

    def delete_available_file(self, file_uuid, timestamp):
        status='SUCCESS'
        reason='SUCCESS'
        filename=self.files[file_uuid][0]
        try:
            path = os.path.join(self.folder, f'{filename}.txt')
            # print(path)
            os.remove(path)
            self.files[file_uuid]=tuple((None, timestamp))
        except:
            status='FAIL'
            reason='FAILED TO DELETE'
        return message.Response(response=status, reason=reason)
    
    def GetAllData(self, request, context):
        data_to_send=message.AllData()
        if request.uuid=='Null':
            for file_uuid in self.files.keys():
                data_to_send.readResponse.append(
                    self.Read(message.ReadDeleteRequest(uuid=file_uuid) , context)
                )
        else:
            data_to_send.readResponse.append(
                    self.Read(message.ReadDeleteRequest(uuid=request.uuid) , context)
                )
        return data_to_send
        pass


def main(address=None):
    if(address==None):
        address='localhost:'+str(get_new_port())
    my_replica=Replica(address=address)
    MAXCLIENTS=10
    try:
        print('-----STARTING REPLICA------')
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=MAXCLIENTS))
        server.add_insecure_port(address)
        servicer.add_ReplicaServicer_to_server(Replica(address),server)
        print("REGISTRY STARTED")
        my_replica.start()
        server.start()
        server.wait_for_termination()
    except KeyboardInterrupt:
        print('-----CLOSING REPLICA------')
        my_replica.stop()
        return
    return


if __name__=='__main__':
    main()