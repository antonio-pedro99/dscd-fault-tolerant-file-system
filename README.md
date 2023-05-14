# Fault Tolerant File-system


## Steps to run the experiment
- Go inside the desired implementation
- run "python run.py" to launch registry and replicas
- run "python client.py" to launch menu driven program for client
- "Ctrl + C" to terminate the run.py


### Generate gRPC code (General)

`python -m grpc_tools.protoc -I../../protos --python_out=. --pyi_out=. --grpc_python_out=. ../../protos/helloworld.proto`



### Generate gRPC code

`python3 -m grpc_tools.protoc -I./ --python_out=. --pyi_out=. --grpc_python_out=. ./backup_protocol.proto`





<!-- to get time
ctime(os.path.getctime(file_path.resolve())) -->
