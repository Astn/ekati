// package: ahghee.grpc
// file: types.proto

/* tslint:disable */
/* eslint-disable */

import * as grpc from "@grpc/grpc-js";
import * as types_pb from "./types_pb";
import * as google_protobuf_timestamp_pb from "google-protobuf/google/protobuf/timestamp_pb";

interface IWatDbServiceService extends grpc.ServiceDefinition<grpc.UntypedServiceImplementation> {
    put: IWatDbServiceService_IPut;
    get: IWatDbServiceService_IGet;
    getMetrics: IWatDbServiceService_IGetMetrics;
    getStats: IWatDbServiceService_IGetStats;
    listStats: IWatDbServiceService_IListStats;
    listPolicies: IWatDbServiceService_IListPolicies;
}

interface IWatDbServiceService_IPut extends grpc.MethodDefinition<types_pb.Node, types_pb.PutResponse> {
    path: string; // "/ahghee.grpc.WatDbService/Put"
    requestStream: boolean; // false
    responseStream: boolean; // false
    requestSerialize: grpc.serialize<types_pb.Node>;
    requestDeserialize: grpc.deserialize<types_pb.Node>;
    responseSerialize: grpc.serialize<types_pb.PutResponse>;
    responseDeserialize: grpc.deserialize<types_pb.PutResponse>;
}
interface IWatDbServiceService_IGet extends grpc.MethodDefinition<types_pb.Query, types_pb.Node> {
    path: string; // "/ahghee.grpc.WatDbService/Get"
    requestStream: boolean; // false
    responseStream: boolean; // true
    requestSerialize: grpc.serialize<types_pb.Query>;
    requestDeserialize: grpc.deserialize<types_pb.Query>;
    responseSerialize: grpc.serialize<types_pb.Node>;
    responseDeserialize: grpc.deserialize<types_pb.Node>;
}
interface IWatDbServiceService_IGetMetrics extends grpc.MethodDefinition<types_pb.GetMetricsRequest, types_pb.GetMetricsResponse> {
    path: string; // "/ahghee.grpc.WatDbService/GetMetrics"
    requestStream: boolean; // false
    responseStream: boolean; // false
    requestSerialize: grpc.serialize<types_pb.GetMetricsRequest>;
    requestDeserialize: grpc.deserialize<types_pb.GetMetricsRequest>;
    responseSerialize: grpc.serialize<types_pb.GetMetricsResponse>;
    responseDeserialize: grpc.deserialize<types_pb.GetMetricsResponse>;
}
interface IWatDbServiceService_IGetStats extends grpc.MethodDefinition<types_pb.GetStatsRequest, types_pb.GetStatsResponse> {
    path: string; // "/ahghee.grpc.WatDbService/GetStats"
    requestStream: boolean; // false
    responseStream: boolean; // false
    requestSerialize: grpc.serialize<types_pb.GetStatsRequest>;
    requestDeserialize: grpc.deserialize<types_pb.GetStatsRequest>;
    responseSerialize: grpc.serialize<types_pb.GetStatsResponse>;
    responseDeserialize: grpc.deserialize<types_pb.GetStatsResponse>;
}
interface IWatDbServiceService_IListStats extends grpc.MethodDefinition<types_pb.ListStatsRequest, types_pb.ListStatsResponse> {
    path: string; // "/ahghee.grpc.WatDbService/ListStats"
    requestStream: boolean; // false
    responseStream: boolean; // false
    requestSerialize: grpc.serialize<types_pb.ListStatsRequest>;
    requestDeserialize: grpc.deserialize<types_pb.ListStatsRequest>;
    responseSerialize: grpc.serialize<types_pb.ListStatsResponse>;
    responseDeserialize: grpc.deserialize<types_pb.ListStatsResponse>;
}
interface IWatDbServiceService_IListPolicies extends grpc.MethodDefinition<types_pb.ListPoliciesRequest, types_pb.Node> {
    path: string; // "/ahghee.grpc.WatDbService/ListPolicies"
    requestStream: boolean; // false
    responseStream: boolean; // true
    requestSerialize: grpc.serialize<types_pb.ListPoliciesRequest>;
    requestDeserialize: grpc.deserialize<types_pb.ListPoliciesRequest>;
    responseSerialize: grpc.serialize<types_pb.Node>;
    responseDeserialize: grpc.deserialize<types_pb.Node>;
}

export const WatDbServiceService: IWatDbServiceService;

export interface IWatDbServiceServer {
    put: grpc.handleUnaryCall<types_pb.Node, types_pb.PutResponse>;
    get: grpc.handleServerStreamingCall<types_pb.Query, types_pb.Node>;
    getMetrics: grpc.handleUnaryCall<types_pb.GetMetricsRequest, types_pb.GetMetricsResponse>;
    getStats: grpc.handleUnaryCall<types_pb.GetStatsRequest, types_pb.GetStatsResponse>;
    listStats: grpc.handleUnaryCall<types_pb.ListStatsRequest, types_pb.ListStatsResponse>;
    listPolicies: grpc.handleServerStreamingCall<types_pb.ListPoliciesRequest, types_pb.Node>;
}

export interface IWatDbServiceClient {
    put(request: types_pb.Node, callback: (error: grpc.ServiceError | null, response: types_pb.PutResponse) => void): grpc.ClientUnaryCall;
    put(request: types_pb.Node, metadata: grpc.Metadata, callback: (error: grpc.ServiceError | null, response: types_pb.PutResponse) => void): grpc.ClientUnaryCall;
    put(request: types_pb.Node, metadata: grpc.Metadata, options: Partial<grpc.CallOptions>, callback: (error: grpc.ServiceError | null, response: types_pb.PutResponse) => void): grpc.ClientUnaryCall;
    get(request: types_pb.Query, options?: Partial<grpc.CallOptions>): grpc.ClientReadableStream<types_pb.Node>;
    get(request: types_pb.Query, metadata?: grpc.Metadata, options?: Partial<grpc.CallOptions>): grpc.ClientReadableStream<types_pb.Node>;
    getMetrics(request: types_pb.GetMetricsRequest, callback: (error: grpc.ServiceError | null, response: types_pb.GetMetricsResponse) => void): grpc.ClientUnaryCall;
    getMetrics(request: types_pb.GetMetricsRequest, metadata: grpc.Metadata, callback: (error: grpc.ServiceError | null, response: types_pb.GetMetricsResponse) => void): grpc.ClientUnaryCall;
    getMetrics(request: types_pb.GetMetricsRequest, metadata: grpc.Metadata, options: Partial<grpc.CallOptions>, callback: (error: grpc.ServiceError | null, response: types_pb.GetMetricsResponse) => void): grpc.ClientUnaryCall;
    getStats(request: types_pb.GetStatsRequest, callback: (error: grpc.ServiceError | null, response: types_pb.GetStatsResponse) => void): grpc.ClientUnaryCall;
    getStats(request: types_pb.GetStatsRequest, metadata: grpc.Metadata, callback: (error: grpc.ServiceError | null, response: types_pb.GetStatsResponse) => void): grpc.ClientUnaryCall;
    getStats(request: types_pb.GetStatsRequest, metadata: grpc.Metadata, options: Partial<grpc.CallOptions>, callback: (error: grpc.ServiceError | null, response: types_pb.GetStatsResponse) => void): grpc.ClientUnaryCall;
    listStats(request: types_pb.ListStatsRequest, callback: (error: grpc.ServiceError | null, response: types_pb.ListStatsResponse) => void): grpc.ClientUnaryCall;
    listStats(request: types_pb.ListStatsRequest, metadata: grpc.Metadata, callback: (error: grpc.ServiceError | null, response: types_pb.ListStatsResponse) => void): grpc.ClientUnaryCall;
    listStats(request: types_pb.ListStatsRequest, metadata: grpc.Metadata, options: Partial<grpc.CallOptions>, callback: (error: grpc.ServiceError | null, response: types_pb.ListStatsResponse) => void): grpc.ClientUnaryCall;
    listPolicies(request: types_pb.ListPoliciesRequest, options?: Partial<grpc.CallOptions>): grpc.ClientReadableStream<types_pb.Node>;
    listPolicies(request: types_pb.ListPoliciesRequest, metadata?: grpc.Metadata, options?: Partial<grpc.CallOptions>): grpc.ClientReadableStream<types_pb.Node>;
}

export class WatDbServiceClient extends grpc.Client implements IWatDbServiceClient {
    constructor(address: string, credentials: grpc.ChannelCredentials, options?: object);
    public put(request: types_pb.Node, callback: (error: grpc.ServiceError | null, response: types_pb.PutResponse) => void): grpc.ClientUnaryCall;
    public put(request: types_pb.Node, metadata: grpc.Metadata, callback: (error: grpc.ServiceError | null, response: types_pb.PutResponse) => void): grpc.ClientUnaryCall;
    public put(request: types_pb.Node, metadata: grpc.Metadata, options: Partial<grpc.CallOptions>, callback: (error: grpc.ServiceError | null, response: types_pb.PutResponse) => void): grpc.ClientUnaryCall;
    public get(request: types_pb.Query, options?: Partial<grpc.CallOptions>): grpc.ClientReadableStream<types_pb.Node>;
    public get(request: types_pb.Query, metadata?: grpc.Metadata, options?: Partial<grpc.CallOptions>): grpc.ClientReadableStream<types_pb.Node>;
    public getMetrics(request: types_pb.GetMetricsRequest, callback: (error: grpc.ServiceError | null, response: types_pb.GetMetricsResponse) => void): grpc.ClientUnaryCall;
    public getMetrics(request: types_pb.GetMetricsRequest, metadata: grpc.Metadata, callback: (error: grpc.ServiceError | null, response: types_pb.GetMetricsResponse) => void): grpc.ClientUnaryCall;
    public getMetrics(request: types_pb.GetMetricsRequest, metadata: grpc.Metadata, options: Partial<grpc.CallOptions>, callback: (error: grpc.ServiceError | null, response: types_pb.GetMetricsResponse) => void): grpc.ClientUnaryCall;
    public getStats(request: types_pb.GetStatsRequest, callback: (error: grpc.ServiceError | null, response: types_pb.GetStatsResponse) => void): grpc.ClientUnaryCall;
    public getStats(request: types_pb.GetStatsRequest, metadata: grpc.Metadata, callback: (error: grpc.ServiceError | null, response: types_pb.GetStatsResponse) => void): grpc.ClientUnaryCall;
    public getStats(request: types_pb.GetStatsRequest, metadata: grpc.Metadata, options: Partial<grpc.CallOptions>, callback: (error: grpc.ServiceError | null, response: types_pb.GetStatsResponse) => void): grpc.ClientUnaryCall;
    public listStats(request: types_pb.ListStatsRequest, callback: (error: grpc.ServiceError | null, response: types_pb.ListStatsResponse) => void): grpc.ClientUnaryCall;
    public listStats(request: types_pb.ListStatsRequest, metadata: grpc.Metadata, callback: (error: grpc.ServiceError | null, response: types_pb.ListStatsResponse) => void): grpc.ClientUnaryCall;
    public listStats(request: types_pb.ListStatsRequest, metadata: grpc.Metadata, options: Partial<grpc.CallOptions>, callback: (error: grpc.ServiceError | null, response: types_pb.ListStatsResponse) => void): grpc.ClientUnaryCall;
    public listPolicies(request: types_pb.ListPoliciesRequest, options?: Partial<grpc.CallOptions>): grpc.ClientReadableStream<types_pb.Node>;
    public listPolicies(request: types_pb.ListPoliciesRequest, metadata?: grpc.Metadata, options?: Partial<grpc.CallOptions>): grpc.ClientReadableStream<types_pb.Node>;
}
