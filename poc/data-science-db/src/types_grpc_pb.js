// GENERATED CODE -- DO NOT EDIT!

'use strict';
var grpc = require('@grpc/grpc-js');
var types_pb = require('./types_pb.js');
var google_protobuf_timestamp_pb = require('google-protobuf/google/protobuf/timestamp_pb.js');

function serialize_ahghee_grpc_GetMetricsRequest(arg) {
  if (!(arg instanceof types_pb.GetMetricsRequest)) {
    throw new Error('Expected argument of type ahghee.grpc.GetMetricsRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_ahghee_grpc_GetMetricsRequest(buffer_arg) {
  return types_pb.GetMetricsRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_ahghee_grpc_GetMetricsResponse(arg) {
  if (!(arg instanceof types_pb.GetMetricsResponse)) {
    throw new Error('Expected argument of type ahghee.grpc.GetMetricsResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_ahghee_grpc_GetMetricsResponse(buffer_arg) {
  return types_pb.GetMetricsResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_ahghee_grpc_GetStatsRequest(arg) {
  if (!(arg instanceof types_pb.GetStatsRequest)) {
    throw new Error('Expected argument of type ahghee.grpc.GetStatsRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_ahghee_grpc_GetStatsRequest(buffer_arg) {
  return types_pb.GetStatsRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_ahghee_grpc_GetStatsResponse(arg) {
  if (!(arg instanceof types_pb.GetStatsResponse)) {
    throw new Error('Expected argument of type ahghee.grpc.GetStatsResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_ahghee_grpc_GetStatsResponse(buffer_arg) {
  return types_pb.GetStatsResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_ahghee_grpc_ListPoliciesRequest(arg) {
  if (!(arg instanceof types_pb.ListPoliciesRequest)) {
    throw new Error('Expected argument of type ahghee.grpc.ListPoliciesRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_ahghee_grpc_ListPoliciesRequest(buffer_arg) {
  return types_pb.ListPoliciesRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_ahghee_grpc_ListStatsRequest(arg) {
  if (!(arg instanceof types_pb.ListStatsRequest)) {
    throw new Error('Expected argument of type ahghee.grpc.ListStatsRequest');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_ahghee_grpc_ListStatsRequest(buffer_arg) {
  return types_pb.ListStatsRequest.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_ahghee_grpc_ListStatsResponse(arg) {
  if (!(arg instanceof types_pb.ListStatsResponse)) {
    throw new Error('Expected argument of type ahghee.grpc.ListStatsResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_ahghee_grpc_ListStatsResponse(buffer_arg) {
  return types_pb.ListStatsResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_ahghee_grpc_Node(arg) {
  if (!(arg instanceof types_pb.Node)) {
    throw new Error('Expected argument of type ahghee.grpc.Node');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_ahghee_grpc_Node(buffer_arg) {
  return types_pb.Node.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_ahghee_grpc_PutResponse(arg) {
  if (!(arg instanceof types_pb.PutResponse)) {
    throw new Error('Expected argument of type ahghee.grpc.PutResponse');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_ahghee_grpc_PutResponse(buffer_arg) {
  return types_pb.PutResponse.deserializeBinary(new Uint8Array(buffer_arg));
}

function serialize_ahghee_grpc_Query(arg) {
  if (!(arg instanceof types_pb.Query)) {
    throw new Error('Expected argument of type ahghee.grpc.Query');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_ahghee_grpc_Query(buffer_arg) {
  return types_pb.Query.deserializeBinary(new Uint8Array(buffer_arg));
}


var WatDbServiceService = exports.WatDbServiceService = {
  put: {
    path: '/ahghee.grpc.WatDbService/Put',
    requestStream: false,
    responseStream: false,
    requestType: types_pb.Node,
    responseType: types_pb.PutResponse,
    requestSerialize: serialize_ahghee_grpc_Node,
    requestDeserialize: deserialize_ahghee_grpc_Node,
    responseSerialize: serialize_ahghee_grpc_PutResponse,
    responseDeserialize: deserialize_ahghee_grpc_PutResponse,
  },
  get: {
    path: '/ahghee.grpc.WatDbService/Get',
    requestStream: false,
    responseStream: true,
    requestType: types_pb.Query,
    responseType: types_pb.Node,
    requestSerialize: serialize_ahghee_grpc_Query,
    requestDeserialize: deserialize_ahghee_grpc_Query,
    responseSerialize: serialize_ahghee_grpc_Node,
    responseDeserialize: deserialize_ahghee_grpc_Node,
  },
  getMetrics: {
    path: '/ahghee.grpc.WatDbService/GetMetrics',
    requestStream: false,
    responseStream: false,
    requestType: types_pb.GetMetricsRequest,
    responseType: types_pb.GetMetricsResponse,
    requestSerialize: serialize_ahghee_grpc_GetMetricsRequest,
    requestDeserialize: deserialize_ahghee_grpc_GetMetricsRequest,
    responseSerialize: serialize_ahghee_grpc_GetMetricsResponse,
    responseDeserialize: deserialize_ahghee_grpc_GetMetricsResponse,
  },
  getStats: {
    path: '/ahghee.grpc.WatDbService/GetStats',
    requestStream: false,
    responseStream: false,
    requestType: types_pb.GetStatsRequest,
    responseType: types_pb.GetStatsResponse,
    requestSerialize: serialize_ahghee_grpc_GetStatsRequest,
    requestDeserialize: deserialize_ahghee_grpc_GetStatsRequest,
    responseSerialize: serialize_ahghee_grpc_GetStatsResponse,
    responseDeserialize: deserialize_ahghee_grpc_GetStatsResponse,
  },
  listStats: {
    path: '/ahghee.grpc.WatDbService/ListStats',
    requestStream: false,
    responseStream: false,
    requestType: types_pb.ListStatsRequest,
    responseType: types_pb.ListStatsResponse,
    requestSerialize: serialize_ahghee_grpc_ListStatsRequest,
    requestDeserialize: deserialize_ahghee_grpc_ListStatsRequest,
    responseSerialize: serialize_ahghee_grpc_ListStatsResponse,
    responseDeserialize: deserialize_ahghee_grpc_ListStatsResponse,
  },
  listPolicies: {
    path: '/ahghee.grpc.WatDbService/ListPolicies',
    requestStream: false,
    responseStream: true,
    requestType: types_pb.ListPoliciesRequest,
    responseType: types_pb.Node,
    requestSerialize: serialize_ahghee_grpc_ListPoliciesRequest,
    requestDeserialize: deserialize_ahghee_grpc_ListPoliciesRequest,
    responseSerialize: serialize_ahghee_grpc_Node,
    responseDeserialize: deserialize_ahghee_grpc_Node,
  },
};

exports.WatDbServiceClient = grpc.makeGenericClientConstructor(WatDbServiceService);
