
use bytes::Bytes;
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
impl Key {
    pub fn new_without_attributes(now:u64, text: &str) -> Key {
        let mut k = Key::new();
        k.set_name(::protobuf::Chars::from(text));
        k.set_time_stamp(now);
        k.clear_edge_attributes();
        k
    }

    pub fn new_with_attributes(now:u64, text: &str, owned_attributes: NodeID) -> Key {
        let mut k = Key::new();
        k.set_name(::protobuf::Chars::from(text));
        k.set_time_stamp(now);
        k.set_edge_attributes(owned_attributes);
        k
    }
}
impl Value {
    pub fn new_with_data(data: Data) -> Value{
        let mut v = Value::new();
        v.set_data(data);
        v
    }
    pub fn new_with_meta_data(meta: Data, data: Data) -> Value{
        let mut v = Value::new();
        v.set_meta_data(meta);
        v.set_data(data);
        v
    }
}