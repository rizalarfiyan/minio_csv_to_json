package utils

import (
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/rizalarfiyan/minio_csv_to_json/constants"
)

func GetMinio() (*minio.Client, error) {
	return minio.New(constants.MinioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(constants.MinioAccessKey, constants.MinioSecretKey, ""),
		Secure: false,
	})
}
