# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: pirestore.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='pirestore.proto',
  package='pirestore',
  syntax='proto3',
  serialized_options=None,
  create_key=_descriptor._internal_create_key,
  serialized_pb=b'\n\x0fpirestore.proto\x12\tpirestore\"W\n\x08Greeting\x12\"\n\x06source\x18\n \x01(\x0b\x32\x12.pirestore.Address\x12\'\n\x0b\x64\x65stination\x18\x0c \x01(\x0b\x32\x12.pirestore.Address\"\xb5\x01\n\x07Request\x12\n\n\x02id\x18\x01 \x01(\x05\x12\x12\n\nreplica_no\x18\x02 \x01(\x05\x12\x0f\n\x07\x63ommand\x18\x03 \x01(\t\x12\x0b\n\x03key\x18\x04 \x01(\x0c\x12\r\n\x05value\x18\x05 \x01(\x0c\x12\x10\n\x08\x65ncoding\x18\x06 \x01(\t\x12\"\n\x06source\x18\n \x01(\x0b\x32\x12.pirestore.Address\x12\'\n\x0b\x64\x65stination\x18\x0c \x01(\x0b\x32\x12.pirestore.Address\"\x89\x01\n\x08Response\x12\x0f\n\x07success\x18\x01 \x01(\x08\x12\r\n\x05value\x18\x02 \x01(\x0c\x12\x10\n\x08\x65ncoding\x18\x03 \x01(\t\x12\"\n\x06source\x18\n \x01(\x0b\x32\x12.pirestore.Address\x12\'\n\x0b\x64\x65stination\x18\x0c \x01(\x0b\x32\x12.pirestore.Address\"c\n\x03\x41\x63k\x12\x0f\n\x07success\x18\x01 \x01(\x08\x12\"\n\x06source\x18\n \x01(\x0b\x32\x12.pirestore.Address\x12\'\n\x0b\x64\x65stination\x18\x0c \x01(\x0b\x32\x12.pirestore.Address\"%\n\x07\x41\x64\x64ress\x12\x0c\n\x04host\x18\x01 \x01(\t\x12\x0c\n\x04port\x18\x02 \x01(\x05\x32s\n\x11PireKeyValueStore\x12.\n\x05Greet\x12\x13.pirestore.Greeting\x1a\x0e.pirestore.Ack\"\x00\x12.\n\x06\x43reate\x12\x12.pirestore.Request\x1a\x0e.pirestore.Ack\"\x00\x62\x06proto3'
)




_GREETING = _descriptor.Descriptor(
  name='Greeting',
  full_name='pirestore.Greeting',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='source', full_name='pirestore.Greeting.source', index=0,
      number=10, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='destination', full_name='pirestore.Greeting.destination', index=1,
      number=12, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=30,
  serialized_end=117,
)


_REQUEST = _descriptor.Descriptor(
  name='Request',
  full_name='pirestore.Request',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='id', full_name='pirestore.Request.id', index=0,
      number=1, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='replica_no', full_name='pirestore.Request.replica_no', index=1,
      number=2, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='command', full_name='pirestore.Request.command', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='key', full_name='pirestore.Request.key', index=3,
      number=4, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value=b"",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='value', full_name='pirestore.Request.value', index=4,
      number=5, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value=b"",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='encoding', full_name='pirestore.Request.encoding', index=5,
      number=6, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='source', full_name='pirestore.Request.source', index=6,
      number=10, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='destination', full_name='pirestore.Request.destination', index=7,
      number=12, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=120,
  serialized_end=301,
)


_RESPONSE = _descriptor.Descriptor(
  name='Response',
  full_name='pirestore.Response',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='success', full_name='pirestore.Response.success', index=0,
      number=1, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='value', full_name='pirestore.Response.value', index=1,
      number=2, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value=b"",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='encoding', full_name='pirestore.Response.encoding', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='source', full_name='pirestore.Response.source', index=3,
      number=10, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='destination', full_name='pirestore.Response.destination', index=4,
      number=12, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=304,
  serialized_end=441,
)


_ACK = _descriptor.Descriptor(
  name='Ack',
  full_name='pirestore.Ack',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='success', full_name='pirestore.Ack.success', index=0,
      number=1, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='source', full_name='pirestore.Ack.source', index=1,
      number=10, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='destination', full_name='pirestore.Ack.destination', index=2,
      number=12, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=443,
  serialized_end=542,
)


_ADDRESS = _descriptor.Descriptor(
  name='Address',
  full_name='pirestore.Address',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='host', full_name='pirestore.Address.host', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='port', full_name='pirestore.Address.port', index=1,
      number=2, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=544,
  serialized_end=581,
)

_GREETING.fields_by_name['source'].message_type = _ADDRESS
_GREETING.fields_by_name['destination'].message_type = _ADDRESS
_REQUEST.fields_by_name['source'].message_type = _ADDRESS
_REQUEST.fields_by_name['destination'].message_type = _ADDRESS
_RESPONSE.fields_by_name['source'].message_type = _ADDRESS
_RESPONSE.fields_by_name['destination'].message_type = _ADDRESS
_ACK.fields_by_name['source'].message_type = _ADDRESS
_ACK.fields_by_name['destination'].message_type = _ADDRESS
DESCRIPTOR.message_types_by_name['Greeting'] = _GREETING
DESCRIPTOR.message_types_by_name['Request'] = _REQUEST
DESCRIPTOR.message_types_by_name['Response'] = _RESPONSE
DESCRIPTOR.message_types_by_name['Ack'] = _ACK
DESCRIPTOR.message_types_by_name['Address'] = _ADDRESS
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

Greeting = _reflection.GeneratedProtocolMessageType('Greeting', (_message.Message,), {
  'DESCRIPTOR' : _GREETING,
  '__module__' : 'pirestore_pb2'
  # @@protoc_insertion_point(class_scope:pirestore.Greeting)
  })
_sym_db.RegisterMessage(Greeting)

Request = _reflection.GeneratedProtocolMessageType('Request', (_message.Message,), {
  'DESCRIPTOR' : _REQUEST,
  '__module__' : 'pirestore_pb2'
  # @@protoc_insertion_point(class_scope:pirestore.Request)
  })
_sym_db.RegisterMessage(Request)

Response = _reflection.GeneratedProtocolMessageType('Response', (_message.Message,), {
  'DESCRIPTOR' : _RESPONSE,
  '__module__' : 'pirestore_pb2'
  # @@protoc_insertion_point(class_scope:pirestore.Response)
  })
_sym_db.RegisterMessage(Response)

Ack = _reflection.GeneratedProtocolMessageType('Ack', (_message.Message,), {
  'DESCRIPTOR' : _ACK,
  '__module__' : 'pirestore_pb2'
  # @@protoc_insertion_point(class_scope:pirestore.Ack)
  })
_sym_db.RegisterMessage(Ack)

Address = _reflection.GeneratedProtocolMessageType('Address', (_message.Message,), {
  'DESCRIPTOR' : _ADDRESS,
  '__module__' : 'pirestore_pb2'
  # @@protoc_insertion_point(class_scope:pirestore.Address)
  })
_sym_db.RegisterMessage(Address)



_PIREKEYVALUESTORE = _descriptor.ServiceDescriptor(
  name='PireKeyValueStore',
  full_name='pirestore.PireKeyValueStore',
  file=DESCRIPTOR,
  index=0,
  serialized_options=None,
  create_key=_descriptor._internal_create_key,
  serialized_start=583,
  serialized_end=698,
  methods=[
  _descriptor.MethodDescriptor(
    name='Greet',
    full_name='pirestore.PireKeyValueStore.Greet',
    index=0,
    containing_service=None,
    input_type=_GREETING,
    output_type=_ACK,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='Create',
    full_name='pirestore.PireKeyValueStore.Create',
    index=1,
    containing_service=None,
    input_type=_REQUEST,
    output_type=_ACK,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
])
_sym_db.RegisterServiceDescriptor(_PIREKEYVALUESTORE)

DESCRIPTOR.services_by_name['PireKeyValueStore'] = _PIREKEYVALUESTORE

# @@protoc_insertion_point(module_scope)
