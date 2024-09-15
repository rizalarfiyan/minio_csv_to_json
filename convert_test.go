package minio_csv_to_json

import (
	"context"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/rizalarfiyan/minio_csv_to_json/utils"
)

var validate *validator.Validate

func init() {
	validate = validator.New()
}

var columns = []Column{
	{
		Name:  "id",
		Rules: "required,numeric",
	},
	{
		Name:  "customer_id",
		Rules: "required",
	},
	{
		Name:  "first_name",
		Rules: "required",
	},
	{
		Name:  "last_name",
		Rules: "required",
	},
	{
		Name:  "company",
		Rules: "required",
	},
	{
		Name:  "city",
		Rules: "required",
	},
	{
		Name:  "country",
		Rules: "required",
	},
	{
		Name:  "phone_1",
		Rules: "required",
	},
	{
		Name:  "phone_2",
		Rules: "required",
	},
	{
		Name:  "email",
		Rules: "required,email",
	},
	{
		Name:  "subscription_date",
		Rules: "required,datetime=2006-01-02",
	},
	{
		Name:  "website",
		Rules: "required,url",
	},
}

func TestConvert(t *testing.T) {
	minio, err := utils.GetMinio()
	if err != nil {
		t.Error(err)
	}

	sheetOpt := NewSheetOption(validate, columns)
	convertOpt := ConvertOption{
		Bucket:   "test-warehouse",
		FileCsv:  "customers-10.csv",
		FileJson: "customers-10.json",
	}

	ctx := context.Background()
	conv := NewConvert(ctx, minio, sheetOpt, convertOpt)
	conv.SetHasHeaderFile(true)
	err = conv.Convert()
	if err != nil {
		t.Error(err)
	}
}
