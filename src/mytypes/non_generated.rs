
use bytes::Bytes;
use std::string::String;
use mytypes::types::*;

impl Data {
    pub fn new_with_string_data(text: &str) -> Data {
        let mut data = Data::new();
        data.set_type_bytes({
            let mut tb = TypeBytes::new();
            tb.set_field_type(::protobuf::Chars::from("xsd:string"));
            tb.set_bytes(Bytes::from(text));
            tb
        });
        data
    }
}

impl KeyValue{
    pub fn new_with_fields(now: u64, key: Data, value: Data) -> KeyValue {
        let mut kv = KeyValue::new();
        kv.set_key({
            let mut tmd = TMD::new();
            tmd.set_time_stamp(now);
            tmd.set_data({
                key
            });
            tmd
        });
        kv.set_value({
            let mut tmd = TMD::new();
            tmd.set_time_stamp(now);
            tmd.set_data({
                value
            });
            tmd
        });
        kv
    }
}