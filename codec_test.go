package rpc

import "testing"

func TestMsgPackCodecUsesWireFieldNames(t *testing.T) {
	data, err := (MsgPackCodec{}).Encode(ServerDescription{
		Name: "plantServer",
		Namespaces: []NamespaceDescription{
			{
				Name:    "calculator",
				Methods: []MethodDescription{{Name: "add", Returns: TypeNumber()}},
				Events:  []EventDescription{},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	value, err := (MsgPackCodec{}).DecodeValue(data)
	if err != nil {
		t.Fatal(err)
	}
	asMap, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("decoded value = %T, want map[string]any", value)
	}
	if _, ok := asMap["name"]; !ok {
		t.Fatalf("decoded keys = %#v, want name", asMap)
	}
	if _, ok := asMap["Name"]; ok {
		t.Fatalf("decoded keys = %#v, did not expect Name", asMap)
	}
}
