# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

import pire.modules.server.pirestore_pb2 as pirestore__pb2


class PireKeyValueStoreStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Read = channel.unary_unary(
                '/pirestore.PireKeyValueStore/Read',
                request_serializer=pirestore__pb2.Request.SerializeToString,
                response_deserializer=pirestore__pb2.Response.FromString,
                )
        self.Prepare = channel.unary_unary(
                '/pirestore.PireKeyValueStore/Prepare',
                request_serializer=pirestore__pb2.Request.SerializeToString,
                response_deserializer=pirestore__pb2.Ack.FromString,
                )
        self.Commit = channel.unary_unary(
                '/pirestore.PireKeyValueStore/Commit',
                request_serializer=pirestore__pb2.Request.SerializeToString,
                response_deserializer=pirestore__pb2.Ack.FromString,
                )
        self.Rollback = channel.unary_unary(
                '/pirestore.PireKeyValueStore/Rollback',
                request_serializer=pirestore__pb2.Request.SerializeToString,
                response_deserializer=pirestore__pb2.Ack.FromString,
                )


class PireKeyValueStoreServicer(object):
    """Missing associated documentation comment in .proto file."""

    def Read(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Prepare(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Commit(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Rollback(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_PireKeyValueStoreServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'Read': grpc.unary_unary_rpc_method_handler(
                    servicer.Read,
                    request_deserializer=pirestore__pb2.Request.FromString,
                    response_serializer=pirestore__pb2.Response.SerializeToString,
            ),
            'Prepare': grpc.unary_unary_rpc_method_handler(
                    servicer.Prepare,
                    request_deserializer=pirestore__pb2.Request.FromString,
                    response_serializer=pirestore__pb2.Ack.SerializeToString,
            ),
            'Commit': grpc.unary_unary_rpc_method_handler(
                    servicer.Commit,
                    request_deserializer=pirestore__pb2.Request.FromString,
                    response_serializer=pirestore__pb2.Ack.SerializeToString,
            ),
            'Rollback': grpc.unary_unary_rpc_method_handler(
                    servicer.Rollback,
                    request_deserializer=pirestore__pb2.Request.FromString,
                    response_serializer=pirestore__pb2.Ack.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'pirestore.PireKeyValueStore', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class PireKeyValueStore(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def Read(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/pirestore.PireKeyValueStore/Read',
            pirestore__pb2.Request.SerializeToString,
            pirestore__pb2.Response.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Prepare(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/pirestore.PireKeyValueStore/Prepare',
            pirestore__pb2.Request.SerializeToString,
            pirestore__pb2.Ack.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Commit(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/pirestore.PireKeyValueStore/Commit',
            pirestore__pb2.Request.SerializeToString,
            pirestore__pb2.Ack.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Rollback(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/pirestore.PireKeyValueStore/Rollback',
            pirestore__pb2.Request.SerializeToString,
            pirestore__pb2.Ack.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
