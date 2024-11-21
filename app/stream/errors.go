package stream

import "errors"

var LessThenPreviousStreamEntryError = errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")

var LessThenAcceptedStreamEntryError = errors.New("ERR The ID specified in XADD must be greater than 0-0")

var WrongIdFormatError = errors.New("Error: specified stream id is in wrong format")
