from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor
FAIL: Status
SUCCESS: Status

class FileDetail(_message.Message):
    __slots__ = ["file_name", "version"]
    FILE_NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    file_name: str
    version: str
    def __init__(self, file_name: _Optional[str] = ..., version: _Optional[str] = ...) -> None: ...

class FileList(_message.Message):
    __slots__ = ["fileList"]
    FILELIST_FIELD_NUMBER: _ClassVar[int]
    fileList: _containers.RepeatedCompositeFieldContainer[FileDetail]
    def __init__(self, fileList: _Optional[_Iterable[_Union[FileDetail, _Mapping]]] = ...) -> None: ...

class ReadDeleteRequest(_message.Message):
    __slots__ = ["uuid"]
    UUID_FIELD_NUMBER: _ClassVar[int]
    uuid: str
    def __init__(self, uuid: _Optional[str] = ...) -> None: ...

class ReadResponse(_message.Message):
    __slots__ = ["content", "name", "status", "version"]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    content: str
    name: str
    status: Status
    version: str
    def __init__(self, status: _Optional[_Union[Status, str]] = ..., name: _Optional[str] = ..., content: _Optional[str] = ..., version: _Optional[str] = ...) -> None: ...

class Response(_message.Message):
    __slots__ = ["reason", "response"]
    REASON_FIELD_NUMBER: _ClassVar[int]
    RESPONSE_FIELD_NUMBER: _ClassVar[int]
    reason: str
    response: Status
    def __init__(self, response: _Optional[_Union[Status, str]] = ..., reason: _Optional[str] = ...) -> None: ...

class ServerListResponse(_message.Message):
    __slots__ = ["serverList"]
    SERVERLIST_FIELD_NUMBER: _ClassVar[int]
    serverList: _containers.RepeatedCompositeFieldContainer[ServerMessage]
    def __init__(self, serverList: _Optional[_Iterable[_Union[ServerMessage, _Mapping]]] = ...) -> None: ...

class ServerMessage(_message.Message):
    __slots__ = ["address", "uuid"]
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    UUID_FIELD_NUMBER: _ClassVar[int]
    address: str
    uuid: str
    def __init__(self, uuid: _Optional[str] = ..., address: _Optional[str] = ...) -> None: ...

class WriteRequest(_message.Message):
    __slots__ = ["content", "name", "uuid"]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    UUID_FIELD_NUMBER: _ClassVar[int]
    content: str
    name: str
    uuid: str
    def __init__(self, name: _Optional[str] = ..., content: _Optional[str] = ..., uuid: _Optional[str] = ...) -> None: ...

class WriteResponse(_message.Message):
    __slots__ = ["status", "uuid", "version"]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    UUID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    status: Status
    uuid: str
    version: str
    def __init__(self, status: _Optional[_Union[Status, str]] = ..., uuid: _Optional[str] = ..., version: _Optional[str] = ...) -> None: ...

class Status(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
