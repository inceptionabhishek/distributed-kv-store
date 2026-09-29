#pragma once
#include "kv_store.hpp"
#include "kvstore.pb.h"
inline Version FromWire(const kvstore::Version& v) { return {v.physical_ms(), v.logical(), v.writer()}; }
inline void ToWire(const Version& v, kvstore::Version* out) {
    out->set_physical_ms(v.physical_ms); out->set_logical(v.logical); out->set_writer(v.writer);
}
inline Entry FromWire(const kvstore::Record& r) {
    return {r.key(), {r.value(), r.version().physical_ms(), FromWire(r.version()), r.tombstone()}};
}
inline void ToWire(const Entry& e, kvstore::Record* out) {
    out->set_key(e.key); out->set_value(e.data.value); ToWire(e.data.version, out->mutable_version());
    out->set_tombstone(e.data.tombstone);
}
inline kvstore::PutRequest PutRequestFor(const Entry& e) {
    kvstore::PutRequest r; r.set_key(e.key); r.set_value(e.data.value);
    ToWire(e.data.version, r.mutable_version()); r.set_tombstone(e.data.tombstone); return r;
}
inline Entry FromWire(const kvstore::PutRequest& r) {
    Version v = r.has_version() ? FromWire(r.version()) : Version{r.timestamp(), 0, "legacy"};
    return {r.key(), {r.value(), v.physical_ms, v, r.tombstone()}};
}
inline void ToWire(const TimestampedValue& v, kvstore::GetResponse* out) {
    out->set_found(!v.tombstone); out->set_value(v.value); out->set_timestamp(v.version.physical_ms);
    ToWire(v.version, out->mutable_version()); out->set_tombstone(v.tombstone);
}
