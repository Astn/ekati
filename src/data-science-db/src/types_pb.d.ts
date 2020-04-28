// package: ahghee.grpc
// file: types.proto

/* tslint:disable */
/* eslint-disable */

import * as jspb from "google-protobuf";
import * as google_protobuf_timestamp_pb from "google-protobuf/google/protobuf/timestamp_pb";

export class MemoryPointer extends jspb.Message { 
    getPartitionkey(): number;
    setPartitionkey(value: number): void;

    getFilename(): number;
    setFilename(value: number): void;

    getOffset(): number;
    setOffset(value: number): void;

    getLength(): number;
    setLength(value: number): void;


    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): MemoryPointer.AsObject;
    static toObject(includeInstance: boolean, msg: MemoryPointer): MemoryPointer.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: MemoryPointer, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): MemoryPointer;
    static deserializeBinaryFromReader(message: MemoryPointer, reader: jspb.BinaryReader): MemoryPointer;
}

export namespace MemoryPointer {
    export type AsObject = {
        partitionkey: number,
        filename: number,
        offset: number,
        length: number,
    }
}

export class TypeBytes extends jspb.Message { 
    getTypeiri(): string;
    setTypeiri(value: string): void;

    getBytes(): Uint8Array | string;
    getBytes_asU8(): Uint8Array;
    getBytes_asB64(): string;
    setBytes(value: Uint8Array | string): void;


    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): TypeBytes.AsObject;
    static toObject(includeInstance: boolean, msg: TypeBytes): TypeBytes.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: TypeBytes, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): TypeBytes;
    static deserializeBinaryFromReader(message: TypeBytes, reader: jspb.BinaryReader): TypeBytes;
}

export namespace TypeBytes {
    export type AsObject = {
        typeiri: string,
        bytes: Uint8Array | string,
    }
}

export class NodeID extends jspb.Message { 
    getRemote(): string;
    setRemote(value: string): void;

    getIri(): string;
    setIri(value: string): void;


    hasPointer(): boolean;
    clearPointer(): void;
    getPointer(): MemoryPointer | undefined;
    setPointer(value?: MemoryPointer): void;


    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): NodeID.AsObject;
    static toObject(includeInstance: boolean, msg: NodeID): NodeID.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: NodeID, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): NodeID;
    static deserializeBinaryFromReader(message: NodeID, reader: jspb.BinaryReader): NodeID;
}

export namespace NodeID {
    export type AsObject = {
        remote: string,
        iri: string,
        pointer?: MemoryPointer.AsObject,
    }
}

export class DataBlock extends jspb.Message { 

    hasNodeid(): boolean;
    clearNodeid(): void;
    getNodeid(): NodeID | undefined;
    setNodeid(value?: NodeID): void;


    hasMetabytes(): boolean;
    clearMetabytes(): void;
    getMetabytes(): TypeBytes | undefined;
    setMetabytes(value?: TypeBytes): void;


    hasStr(): boolean;
    clearStr(): void;
    getStr(): string;
    setStr(value: string): void;


    hasI32(): boolean;
    clearI32(): void;
    getI32(): number;
    setI32(value: number): void;


    hasI64(): boolean;
    clearI64(): void;
    getI64(): number;
    setI64(value: number): void;


    hasUi32(): boolean;
    clearUi32(): void;
    getUi32(): number;
    setUi32(value: number): void;


    hasUi64(): boolean;
    clearUi64(): void;
    getUi64(): number;
    setUi64(value: number): void;


    hasD(): boolean;
    clearD(): void;
    getD(): number;
    setD(value: number): void;


    hasF(): boolean;
    clearF(): void;
    getF(): number;
    setF(value: number): void;


    hasB(): boolean;
    clearB(): void;
    getB(): boolean;
    setB(value: boolean): void;


    hasMemorypointer(): boolean;
    clearMemorypointer(): void;
    getMemorypointer(): MemoryPointer | undefined;
    setMemorypointer(value?: MemoryPointer): void;


    getDataCase(): DataBlock.DataCase;

    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): DataBlock.AsObject;
    static toObject(includeInstance: boolean, msg: DataBlock): DataBlock.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: DataBlock, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): DataBlock;
    static deserializeBinaryFromReader(message: DataBlock, reader: jspb.BinaryReader): DataBlock;
}

export namespace DataBlock {
    export type AsObject = {
        nodeid?: NodeID.AsObject,
        metabytes?: TypeBytes.AsObject,
        str: string,
        i32: number,
        i64: number,
        ui32: number,
        ui64: number,
        d: number,
        f: number,
        b: boolean,
        memorypointer?: MemoryPointer.AsObject,
    }

    export enum DataCase {
        DATA_NOT_SET = 0,
    
    NODEID = 1,

    METABYTES = 2,

    STR = 3,

    I32 = 4,

    I64 = 5,

    UI32 = 6,

    UI64 = 7,

    D = 8,

    F = 9,

    B = 10,

    MEMORYPOINTER = 11,

    }

}

export class TMD extends jspb.Message { 
    getTimestamp(): number;
    setTimestamp(value: number): void;


    hasMetadata(): boolean;
    clearMetadata(): void;
    getMetadata(): DataBlock | undefined;
    setMetadata(value?: DataBlock): void;


    hasData(): boolean;
    clearData(): void;
    getData(): DataBlock | undefined;
    setData(value?: DataBlock): void;


    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): TMD.AsObject;
    static toObject(includeInstance: boolean, msg: TMD): TMD.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: TMD, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): TMD;
    static deserializeBinaryFromReader(message: TMD, reader: jspb.BinaryReader): TMD;
}

export namespace TMD {
    export type AsObject = {
        timestamp: number,
        metadata?: DataBlock.AsObject,
        data?: DataBlock.AsObject,
    }
}

export class KeyValue extends jspb.Message { 

    hasKey(): boolean;
    clearKey(): void;
    getKey(): TMD | undefined;
    setKey(value?: TMD): void;


    hasValue(): boolean;
    clearValue(): void;
    getValue(): TMD | undefined;
    setValue(value?: TMD): void;


    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): KeyValue.AsObject;
    static toObject(includeInstance: boolean, msg: KeyValue): KeyValue.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: KeyValue, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): KeyValue;
    static deserializeBinaryFromReader(message: KeyValue, reader: jspb.BinaryReader): KeyValue;
}

export namespace KeyValue {
    export type AsObject = {
        key?: TMD.AsObject,
        value?: TMD.AsObject,
    }
}

export class Node extends jspb.Message { 

    hasId(): boolean;
    clearId(): void;
    getId(): NodeID | undefined;
    setId(value?: NodeID): void;

    clearFragmentsList(): void;
    getFragmentsList(): Array<MemoryPointer>;
    setFragmentsList(value: Array<MemoryPointer>): void;
    addFragments(value?: MemoryPointer, index?: number): MemoryPointer;

    clearAttributesList(): void;
    getAttributesList(): Array<KeyValue>;
    setAttributesList(value: Array<KeyValue>): void;
    addAttributes(value?: KeyValue, index?: number): KeyValue;


    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): Node.AsObject;
    static toObject(includeInstance: boolean, msg: Node): Node.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: Node, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): Node;
    static deserializeBinaryFromReader(message: Node, reader: jspb.BinaryReader): Node;
}

export namespace Node {
    export type AsObject = {
        id?: NodeID.AsObject,
        fragmentsList: Array<MemoryPointer.AsObject>,
        attributesList: Array<KeyValue.AsObject>,
    }
}

export class Pointers extends jspb.Message { 
    clearPointersList(): void;
    getPointersList(): Array<MemoryPointer>;
    setPointersList(value: Array<MemoryPointer>): void;
    addPointers(value?: MemoryPointer, index?: number): MemoryPointer;


    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): Pointers.AsObject;
    static toObject(includeInstance: boolean, msg: Pointers): Pointers.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: Pointers, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): Pointers;
    static deserializeBinaryFromReader(message: Pointers, reader: jspb.BinaryReader): Pointers;
}

export namespace Pointers {
    export type AsObject = {
        pointersList: Array<MemoryPointer.AsObject>,
    }
}

export class Range extends jspb.Message { 
    getFrom(): number;
    setFrom(value: number): void;

    getTo(): number;
    setTo(value: number): void;


    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): Range.AsObject;
    static toObject(includeInstance: boolean, msg: Range): Range.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: Range, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): Range;
    static deserializeBinaryFromReader(message: Range, reader: jspb.BinaryReader): Range;
}

export namespace Range {
    export type AsObject = {
        from: number,
        to: number,
    }
}

export class FollowOperator extends jspb.Message { 

    hasFollowany(): boolean;
    clearFollowany(): void;
    getFollowany(): FollowOperator.FollowAny | undefined;
    setFollowany(value?: FollowOperator.FollowAny): void;


    hasFollowedge(): boolean;
    clearFollowedge(): void;
    getFollowedge(): FollowOperator.EdgeNum | undefined;
    setFollowedge(value?: FollowOperator.EdgeNum): void;


    getFollowCase(): FollowOperator.FollowCase;

    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): FollowOperator.AsObject;
    static toObject(includeInstance: boolean, msg: FollowOperator): FollowOperator.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: FollowOperator, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): FollowOperator;
    static deserializeBinaryFromReader(message: FollowOperator, reader: jspb.BinaryReader): FollowOperator;
}

export namespace FollowOperator {
    export type AsObject = {
        followany?: FollowOperator.FollowAny.AsObject,
        followedge?: FollowOperator.EdgeNum.AsObject,
    }


    export class FollowAny extends jspb.Message { 

        hasRange(): boolean;
        clearRange(): void;
        getRange(): Range | undefined;
        setRange(value?: Range): void;


        serializeBinary(): Uint8Array;
        toObject(includeInstance?: boolean): FollowAny.AsObject;
        static toObject(includeInstance: boolean, msg: FollowAny): FollowAny.AsObject;
        static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
        static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
        static serializeBinaryToWriter(message: FollowAny, writer: jspb.BinaryWriter): void;
        static deserializeBinary(bytes: Uint8Array): FollowAny;
        static deserializeBinaryFromReader(message: FollowAny, reader: jspb.BinaryReader): FollowAny;
    }

    export namespace FollowAny {
        export type AsObject = {
            range?: Range.AsObject,
        }
    }

    export class EdgeRange extends jspb.Message { 

        hasEdge(): boolean;
        clearEdge(): void;
        getEdge(): DataBlock | undefined;
        setEdge(value?: DataBlock): void;


        hasRange(): boolean;
        clearRange(): void;
        getRange(): Range | undefined;
        setRange(value?: Range): void;


        serializeBinary(): Uint8Array;
        toObject(includeInstance?: boolean): EdgeRange.AsObject;
        static toObject(includeInstance: boolean, msg: EdgeRange): EdgeRange.AsObject;
        static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
        static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
        static serializeBinaryToWriter(message: EdgeRange, writer: jspb.BinaryWriter): void;
        static deserializeBinary(bytes: Uint8Array): EdgeRange;
        static deserializeBinaryFromReader(message: EdgeRange, reader: jspb.BinaryReader): EdgeRange;
    }

    export namespace EdgeRange {
        export type AsObject = {
            edge?: DataBlock.AsObject,
            range?: Range.AsObject,
        }
    }

    export class EdgeCMP extends jspb.Message { 

        hasLeft(): boolean;
        clearLeft(): void;
        getLeft(): FollowOperator.EdgeNum | undefined;
        setLeft(value?: FollowOperator.EdgeNum): void;

        getBoolop(): string;
        setBoolop(value: string): void;


        hasRight(): boolean;
        clearRight(): void;
        getRight(): FollowOperator.EdgeNum | undefined;
        setRight(value?: FollowOperator.EdgeNum): void;


        serializeBinary(): Uint8Array;
        toObject(includeInstance?: boolean): EdgeCMP.AsObject;
        static toObject(includeInstance: boolean, msg: EdgeCMP): EdgeCMP.AsObject;
        static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
        static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
        static serializeBinaryToWriter(message: EdgeCMP, writer: jspb.BinaryWriter): void;
        static deserializeBinary(bytes: Uint8Array): EdgeCMP;
        static deserializeBinaryFromReader(message: EdgeCMP, reader: jspb.BinaryReader): EdgeCMP;
    }

    export namespace EdgeCMP {
        export type AsObject = {
            left?: FollowOperator.EdgeNum.AsObject,
            boolop: string,
            right?: FollowOperator.EdgeNum.AsObject,
        }
    }

    export class EdgeNum extends jspb.Message { 

        hasEdgerange(): boolean;
        clearEdgerange(): void;
        getEdgerange(): FollowOperator.EdgeRange | undefined;
        setEdgerange(value?: FollowOperator.EdgeRange): void;


        hasEdgecmp(): boolean;
        clearEdgecmp(): void;
        getEdgecmp(): FollowOperator.EdgeCMP | undefined;
        setEdgecmp(value?: FollowOperator.EdgeCMP): void;


        getOpCase(): EdgeNum.OpCase;

        serializeBinary(): Uint8Array;
        toObject(includeInstance?: boolean): EdgeNum.AsObject;
        static toObject(includeInstance: boolean, msg: EdgeNum): EdgeNum.AsObject;
        static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
        static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
        static serializeBinaryToWriter(message: EdgeNum, writer: jspb.BinaryWriter): void;
        static deserializeBinary(bytes: Uint8Array): EdgeNum;
        static deserializeBinaryFromReader(message: EdgeNum, reader: jspb.BinaryReader): EdgeNum;
    }

    export namespace EdgeNum {
        export type AsObject = {
            edgerange?: FollowOperator.EdgeRange.AsObject,
            edgecmp?: FollowOperator.EdgeCMP.AsObject,
        }

        export enum OpCase {
            OP_NOT_SET = 0,
        
    EDGERANGE = 1,

    EDGECMP = 2,

        }

    }


    export enum FollowCase {
        FOLLOW_NOT_SET = 0,
    
    FOLLOWANY = 1,

    FOLLOWEDGE = 2,

    }

}

export class FilterOperator extends jspb.Message { 

    hasCompare(): boolean;
    clearCompare(): void;
    getCompare(): FilterOperator.Compare | undefined;
    setCompare(value?: FilterOperator.Compare): void;


    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): FilterOperator.AsObject;
    static toObject(includeInstance: boolean, msg: FilterOperator): FilterOperator.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: FilterOperator, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): FilterOperator;
    static deserializeBinaryFromReader(message: FilterOperator, reader: jspb.BinaryReader): FilterOperator;
}

export namespace FilterOperator {
    export type AsObject = {
        compare?: FilterOperator.Compare.AsObject,
    }


    export class Compare extends jspb.Message { 

        hasKevvaluecmp(): boolean;
        clearKevvaluecmp(): void;
        getKevvaluecmp(): FilterOperator.CompareKeyValue | undefined;
        setKevvaluecmp(value?: FilterOperator.CompareKeyValue): void;


        hasCompoundcmp(): boolean;
        clearCompoundcmp(): void;
        getCompoundcmp(): FilterOperator.CompareCompound | undefined;
        setCompoundcmp(value?: FilterOperator.CompareCompound): void;


        getCmpTypeCase(): Compare.CmpTypeCase;

        serializeBinary(): Uint8Array;
        toObject(includeInstance?: boolean): Compare.AsObject;
        static toObject(includeInstance: boolean, msg: Compare): Compare.AsObject;
        static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
        static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
        static serializeBinaryToWriter(message: Compare, writer: jspb.BinaryWriter): void;
        static deserializeBinary(bytes: Uint8Array): Compare;
        static deserializeBinaryFromReader(message: Compare, reader: jspb.BinaryReader): Compare;
    }

    export namespace Compare {
        export type AsObject = {
            kevvaluecmp?: FilterOperator.CompareKeyValue.AsObject,
            compoundcmp?: FilterOperator.CompareCompound.AsObject,
        }

        export enum CmpTypeCase {
            CMPTYPE_NOT_SET = 0,
        
    KEVVALUECMP = 1,

    COMPOUNDCMP = 2,

        }

    }

    export class CompareKeyValue extends jspb.Message { 

        hasProperty(): boolean;
        clearProperty(): void;
        getProperty(): DataBlock | undefined;
        setProperty(value?: DataBlock): void;

        getMathop(): string;
        setMathop(value: string): void;


        hasValue(): boolean;
        clearValue(): void;
        getValue(): DataBlock | undefined;
        setValue(value?: DataBlock): void;


        serializeBinary(): Uint8Array;
        toObject(includeInstance?: boolean): CompareKeyValue.AsObject;
        static toObject(includeInstance: boolean, msg: CompareKeyValue): CompareKeyValue.AsObject;
        static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
        static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
        static serializeBinaryToWriter(message: CompareKeyValue, writer: jspb.BinaryWriter): void;
        static deserializeBinary(bytes: Uint8Array): CompareKeyValue;
        static deserializeBinaryFromReader(message: CompareKeyValue, reader: jspb.BinaryReader): CompareKeyValue;
    }

    export namespace CompareKeyValue {
        export type AsObject = {
            property?: DataBlock.AsObject,
            mathop: string,
            value?: DataBlock.AsObject,
        }
    }

    export class CompareCompound extends jspb.Message { 

        hasLeft(): boolean;
        clearLeft(): void;
        getLeft(): FilterOperator.Compare | undefined;
        setLeft(value?: FilterOperator.Compare): void;

        getBoolop(): string;
        setBoolop(value: string): void;


        hasRight(): boolean;
        clearRight(): void;
        getRight(): FilterOperator.Compare | undefined;
        setRight(value?: FilterOperator.Compare): void;


        serializeBinary(): Uint8Array;
        toObject(includeInstance?: boolean): CompareCompound.AsObject;
        static toObject(includeInstance: boolean, msg: CompareCompound): CompareCompound.AsObject;
        static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
        static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
        static serializeBinaryToWriter(message: CompareCompound, writer: jspb.BinaryWriter): void;
        static deserializeBinary(bytes: Uint8Array): CompareCompound;
        static deserializeBinaryFromReader(message: CompareCompound, reader: jspb.BinaryReader): CompareCompound;
    }

    export namespace CompareCompound {
        export type AsObject = {
            left?: FilterOperator.Compare.AsObject,
            boolop: string,
            right?: FilterOperator.Compare.AsObject,
        }
    }

}

export class Step extends jspb.Message { 

    hasFollow(): boolean;
    clearFollow(): void;
    getFollow(): FollowOperator | undefined;
    setFollow(value?: FollowOperator): void;


    hasWhere(): boolean;
    clearWhere(): void;
    getWhere(): FilterOperator | undefined;
    setWhere(value?: FilterOperator): void;


    hasNext(): boolean;
    clearNext(): void;
    getNext(): Step | undefined;
    setNext(value?: Step): void;


    getOperatorCase(): Step.OperatorCase;

    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): Step.AsObject;
    static toObject(includeInstance: boolean, msg: Step): Step.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: Step, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): Step;
    static deserializeBinaryFromReader(message: Step, reader: jspb.BinaryReader): Step;
}

export namespace Step {
    export type AsObject = {
        follow?: FollowOperator.AsObject,
        where?: FilterOperator.AsObject,
        next?: Step.AsObject,
    }

    export enum OperatorCase {
        OPERATOR_NOT_SET = 0,
    
    FOLLOW = 1,

    WHERE = 2,

    }

}

export class Query extends jspb.Message { 

    hasStep(): boolean;
    clearStep(): void;
    getStep(): Step | undefined;
    setStep(value?: Step): void;

    clearIrisList(): void;
    getIrisList(): Array<string>;
    setIrisList(value: Array<string>): void;
    addIris(value: string, index?: number): string;


    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): Query.AsObject;
    static toObject(includeInstance: boolean, msg: Query): Query.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: Query, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): Query;
    static deserializeBinaryFromReader(message: Query, reader: jspb.BinaryReader): Query;
}

export namespace Query {
    export type AsObject = {
        step?: Step.AsObject,
        irisList: Array<string>,
    }
}

export class PutResponse extends jspb.Message { 
    getSuccess(): boolean;
    setSuccess(value: boolean): void;


    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): PutResponse.AsObject;
    static toObject(includeInstance: boolean, msg: PutResponse): PutResponse.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: PutResponse, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): PutResponse;
    static deserializeBinaryFromReader(message: PutResponse, reader: jspb.BinaryReader): PutResponse;
}

export namespace PutResponse {
    export type AsObject = {
        success: boolean,
    }
}

export class GetMetricsResponse extends jspb.Message { 
    clearMetricsList(): void;
    getMetricsList(): Array<GetMetricsResponse.Metric>;
    setMetricsList(value: Array<GetMetricsResponse.Metric>): void;
    addMetrics(value?: GetMetricsResponse.Metric, index?: number): GetMetricsResponse.Metric;


    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): GetMetricsResponse.AsObject;
    static toObject(includeInstance: boolean, msg: GetMetricsResponse): GetMetricsResponse.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: GetMetricsResponse, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): GetMetricsResponse;
    static deserializeBinaryFromReader(message: GetMetricsResponse, reader: jspb.BinaryReader): GetMetricsResponse;
}

export namespace GetMetricsResponse {
    export type AsObject = {
        metricsList: Array<GetMetricsResponse.Metric.AsObject>,
    }


    export class Metric extends jspb.Message { 
        getValue(): number;
        setValue(value: number): void;

        getName(): string;
        setName(value: string): void;


        hasTime(): boolean;
        clearTime(): void;
        getTime(): google_protobuf_timestamp_pb.Timestamp | undefined;
        setTime(value?: google_protobuf_timestamp_pb.Timestamp): void;


        serializeBinary(): Uint8Array;
        toObject(includeInstance?: boolean): Metric.AsObject;
        static toObject(includeInstance: boolean, msg: Metric): Metric.AsObject;
        static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
        static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
        static serializeBinaryToWriter(message: Metric, writer: jspb.BinaryWriter): void;
        static deserializeBinary(bytes: Uint8Array): Metric;
        static deserializeBinaryFromReader(message: Metric, reader: jspb.BinaryReader): Metric;
    }

    export namespace Metric {
        export type AsObject = {
            value: number,
            name: string,
            time?: google_protobuf_timestamp_pb.Timestamp.AsObject,
        }
    }

}

export class GetMetricsRequest extends jspb.Message { 
    clearNamesList(): void;
    getNamesList(): Array<string>;
    setNamesList(value: Array<string>): void;
    addNames(value: string, index?: number): string;


    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): GetMetricsRequest.AsObject;
    static toObject(includeInstance: boolean, msg: GetMetricsRequest): GetMetricsRequest.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: GetMetricsRequest, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): GetMetricsRequest;
    static deserializeBinaryFromReader(message: GetMetricsRequest, reader: jspb.BinaryReader): GetMetricsRequest;
}

export namespace GetMetricsRequest {
    export type AsObject = {
        namesList: Array<string>,
    }
}

export class GetStatsRequest extends jspb.Message { 
    clearStatsList(): void;
    getStatsList(): Array<GetStatsRequest.Stat>;
    setStatsList(value: Array<GetStatsRequest.Stat>): void;
    addStats(value?: GetStatsRequest.Stat, index?: number): GetStatsRequest.Stat;


    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): GetStatsRequest.AsObject;
    static toObject(includeInstance: boolean, msg: GetStatsRequest): GetStatsRequest.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: GetStatsRequest, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): GetStatsRequest;
    static deserializeBinaryFromReader(message: GetStatsRequest, reader: jspb.BinaryReader): GetStatsRequest;
}

export namespace GetStatsRequest {
    export type AsObject = {
        statsList: Array<GetStatsRequest.Stat.AsObject>,
    }


    export class Stat extends jspb.Message { 
        getValue(): number;
        setValue(value: number): void;

        getTopic(): string;
        setTopic(value: string): void;

        getStat(): string;
        setStat(value: string): void;


        serializeBinary(): Uint8Array;
        toObject(includeInstance?: boolean): Stat.AsObject;
        static toObject(includeInstance: boolean, msg: Stat): Stat.AsObject;
        static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
        static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
        static serializeBinaryToWriter(message: Stat, writer: jspb.BinaryWriter): void;
        static deserializeBinary(bytes: Uint8Array): Stat;
        static deserializeBinaryFromReader(message: Stat, reader: jspb.BinaryReader): Stat;
    }

    export namespace Stat {
        export type AsObject = {
            value: number,
            topic: string,
            stat: string,
        }
    }

}

export class GetStatsResponse extends jspb.Message { 
    clearNamesList(): void;
    getNamesList(): Array<string>;
    setNamesList(value: Array<string>): void;
    addNames(value: string, index?: number): string;


    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): GetStatsResponse.AsObject;
    static toObject(includeInstance: boolean, msg: GetStatsResponse): GetStatsResponse.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: GetStatsResponse, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): GetStatsResponse;
    static deserializeBinaryFromReader(message: GetStatsResponse, reader: jspb.BinaryReader): GetStatsResponse;
}

export namespace GetStatsResponse {
    export type AsObject = {
        namesList: Array<string>,
    }
}

export class ListStatsRequest extends jspb.Message { 
    clearMatchList(): void;
    getMatchList(): Array<string>;
    setMatchList(value: Array<string>): void;
    addMatch(value: string, index?: number): string;


    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): ListStatsRequest.AsObject;
    static toObject(includeInstance: boolean, msg: ListStatsRequest): ListStatsRequest.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: ListStatsRequest, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): ListStatsRequest;
    static deserializeBinaryFromReader(message: ListStatsRequest, reader: jspb.BinaryReader): ListStatsRequest;
}

export namespace ListStatsRequest {
    export type AsObject = {
        matchList: Array<string>,
    }
}

export class ListStatsResponse extends jspb.Message { 
    clearNamesList(): void;
    getNamesList(): Array<string>;
    setNamesList(value: Array<string>): void;
    addNames(value: string, index?: number): string;


    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): ListStatsResponse.AsObject;
    static toObject(includeInstance: boolean, msg: ListStatsResponse): ListStatsResponse.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: ListStatsResponse, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): ListStatsResponse;
    static deserializeBinaryFromReader(message: ListStatsResponse, reader: jspb.BinaryReader): ListStatsResponse;
}

export namespace ListStatsResponse {
    export type AsObject = {
        namesList: Array<string>,
    }
}

export class ListPoliciesRequest extends jspb.Message { 
    clearIrisList(): void;
    getIrisList(): Array<string>;
    setIrisList(value: Array<string>): void;
    addIris(value: string, index?: number): string;


    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): ListPoliciesRequest.AsObject;
    static toObject(includeInstance: boolean, msg: ListPoliciesRequest): ListPoliciesRequest.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: ListPoliciesRequest, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): ListPoliciesRequest;
    static deserializeBinaryFromReader(message: ListPoliciesRequest, reader: jspb.BinaryReader): ListPoliciesRequest;
}

export namespace ListPoliciesRequest {
    export type AsObject = {
        irisList: Array<string>,
    }
}

export class ListPoliciesResponse extends jspb.Message { 
    clearNodesList(): void;
    getNodesList(): Array<Node>;
    setNodesList(value: Array<Node>): void;
    addNodes(value?: Node, index?: number): Node;


    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): ListPoliciesResponse.AsObject;
    static toObject(includeInstance: boolean, msg: ListPoliciesResponse): ListPoliciesResponse.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: ListPoliciesResponse, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): ListPoliciesResponse;
    static deserializeBinaryFromReader(message: ListPoliciesResponse, reader: jspb.BinaryReader): ListPoliciesResponse;
}

export namespace ListPoliciesResponse {
    export type AsObject = {
        nodesList: Array<Node.AsObject>,
    }
}
