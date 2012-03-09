namespace java com.rubiconproject.data.thrift.types

struct TStringMap {
 1: map<string, string> values
}

struct TIntegerMap {
 1: map<i32, i32> values
}

struct TDoubleMap {
 1: map<string, double> values
}

struct TStringIntegerMap {
 1: map<string, i32> values
}

struct TIntegerLongMap {
 1: map<i32, i64> values
}
