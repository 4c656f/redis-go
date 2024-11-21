package xaddcommand

import "errors"

var NotEnoughValuesToConstructArgsError = errors.New("Error: values len is len then minimum to construct xadd args")
