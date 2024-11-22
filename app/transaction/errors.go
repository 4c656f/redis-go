package transaction

import "errors"

var DiscardWithoutMultiError = errors.New("ERR DISCARD without MULTI")
var ExecWithoutMultiError = errors.New("ERR EXEC without MULTI")
