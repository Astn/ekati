
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
    pub fn new_with_node_id(node_id:NodeID) -> Data {
        let mut data = Data::new();
        data.set_node_id(node_id);
        data
    }
    pub fn new_with_global_node_id(global_node_id:GlobalNodeID) -> Data{
        let mut data = Data::new();
        data.set_global_node_id(global_node_id);
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
impl NodeID {
    pub fn new_with_graph_and_id(graph: &str, id: &str) -> NodeID{
        let mut nid = NodeID::new();
        // todo: better way to make chars directly from a string maybe Chars::From<String>(...)
        nid.set_graph(::protobuf::Chars::from(graph));
        nid.set_nodeid(::protobuf::Chars::from(id));
        nid
    }
}
impl AddressBlock {
    pub fn new_with_data(graph: &str, node_id: &str) -> AddressBlock {
        let mut ab = AddressBlock::new();
        ab.set_node_id({
            NodeID::new_with_graph_and_id(graph, node_id)
        });
        ab
    }
}
impl Node_Fragment {
    pub fn add_simple_property(&mut self, now:u64, key: &str, value: &str){
        self.mut_keys().push(Key::new_without_attributes(now, key));
        self.mut_values().push(Value::new_with_data(Data::new_with_string_data(value)));
    }
    pub fn add_simple_edge(&mut self, now:u64, key: &str, value: NodeID){
        self.mut_keys().push(Key::new_without_attributes(now, key));
        self.mut_values().push(Value::new_with_data(Data::new_with_node_id(value)));
    }
}