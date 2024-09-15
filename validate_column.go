package minio_csv_to_json

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/go-playground/validator/v10"
)

type SheetOption interface {
	GetColumnLength() int
	Column(idx int) Column
	ValidateRow(rows []string) ([]byte, error)
}

type sheetOption struct {
	columns   []Column
	validator *validator.Validate
}

func NewSheetOption(validator *validator.Validate, columns []Column) SheetOption {
	opt := sheetOption{
		validator: validator,
		columns:   columns,
	}
	opt.init()
	return &opt
}

func (so *sheetOption) GetColumnLength() int {
	return len(so.columns)
}

func (so *sheetOption) Column(idx int) Column {
	return so.columns[idx]
}

func (so *sheetOption) ValidateRow(rows []string) ([]byte, error) {
	var bt bytes.Buffer
	bt.WriteRune('{')
	maxColumn := so.GetColumnLength()
	for idx, column := range so.columns {
		row := rows[idx]
		err := column.Validate(so.validator, row)
		if err != nil {
			return nil, err
		}

		if idx > 0 && idx < maxColumn {
			bt.WriteRune(',')
		}

		bt.Write(column.nameValue())
		bt.WriteRune(':')
		bt.Write(column.writeValue(row))
	}
	bt.WriteRune('}')
	return bt.Bytes(), nil
}

func (so *sheetOption) init() {
	for i := range so.columns {
		so.columns[i].isNoQuoteJson = so.columns[i].updateIsNoQuoteJson()
	}
}

type Column struct {
	Name  string `json:"name"`
	Rules string `json:"rules"`

	isNoQuoteJson bool
}

func (c *Column) Validate(validate *validator.Validate, value string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			//! TODO: check if error is no have type validation error
			err = fmt.Errorf("%v", r)
		}
	}()
	err = validate.Var(value, c.Rules)
	return
}

func (c *Column) updateIsNoQuoteJson() bool {
	// if the column is numeric, number, or bool, then it should not be quoted
	// the rules is based on the go-playground/validator
	for _, dic := range []string{"numeric", "number", "bool"} {
		if strings.Contains(c.Rules, dic) {
			return true
		}
	}

	return false
}

func (c *Column) withQuote(value string) []byte {
	return []byte(fmt.Sprintf(`"%s"`, value))
}

func (c *Column) nameValue() []byte {
	return c.withQuote(c.Name)
}

func (c *Column) writeValue(value string) []byte {
	if c.isNoQuoteJson {
		return []byte(value)
	}

	return c.withQuote(value)
}
