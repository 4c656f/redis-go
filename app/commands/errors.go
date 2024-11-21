package commands

import "errors"

var UnknownArgTypeCastError = errors.New("Error trying cast arg to type: this type is not supported by args")

var WrongArgTypeCastError = errors.New("Error trying cast arg to type: argument holds value of different type")

var GetUnknowArgError = errors.New("Error trying cast arg to type: argument holds value of different type")
