package types

// RawEvent is a generic placeholder type to represent any event value
// (raw parsed structs or normalized events). Plugins may type-assert
// into concrete types as needed.
type RawEvent = interface{}
