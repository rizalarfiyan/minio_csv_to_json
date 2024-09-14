package utils

import (
	"fmt"
	"os"
	"path"
)

type sample struct {
	fileName string
	long     int
}

type Sample interface {
	FileName() string
	Long() int
	FullPath() string
}

func NewSample(long int) Sample {
	return &sample{
		long:     long,
		fileName: fmt.Sprint("customers-", long, ".csv"),
	}
}

func (s *sample) FileName() string {
	return s.fileName
}

func (s *sample) Long() int {
	return s.long
}

func (s *sample) FullPath() string {
	pwd, err := os.Getwd()
	if err != nil {
		return ""
	}

	return path.Join(pwd, "..", "sample", s.fileName)
}
