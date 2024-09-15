package reader

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/rizalarfiyan/minio_csv_to_json/constants"
	"github.com/rizalarfiyan/minio_csv_to_json/utils"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func init() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
}

const (
	bufferSize      = 4 * 1024
	fileHasHeader   = true
	csvDelimiter    = ','
	csvColumnsCount = 12
)

func TestSeekerMinio(t *testing.T) {
	minioClient, err := utils.GetMinio()
	if err != nil {
		t.Error(err)
	}

	sample := utils.NewSample(9)
	ctx := context.Background()
	object, err := minioClient.GetObject(ctx, constants.MinioBucket, sample.FileName(), minio.GetObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func(object *minio.Object) {
		err := object.Close()
		if err != nil {
			t.Error(err)
		}
	}(object)

	stat, err := object.Stat()
	if err != nil {
		t.Error(err)
	}

	// assumed if the offsetEnd is not finish of csv file
	seeker(object, 100, int(stat.Size))
}

func seeker(object *minio.Object, offsetStart, offsetEnd int) {
	var line []byte

	r := bufio.NewReaderSize(object, bufferSize)
	_, _ = object.Seek(int64(offsetStart), io.SeekStart)

	// remove if file have header, and skipping the main data
	if fileHasHeader {
		line = readLine(r)
		if line == nil {
			return
		}
	}

	realOffsetStart := offsetStart + len(line)
	currentOffsetPos := realOffsetStart

	bytesReader := bytes.NewReader(line)
	csvReader := csv.NewReader(bytesReader)
	csvReader.Comma = csvDelimiter
	csvReader.FieldsPerRecord = csvColumnsCount

	var counter int
	for {
		line = readLine(r)
		if line == nil {
			break
		}

		offsetStart += len(line)
		counter++

		bytesReader.Reset(line)
		record, err := csvReader.Read()
		if err != nil {
			log.Warn().Err(err).Msg("Error reading csv file")
		}

		log.Info().Int("start", offsetStart).Int("end", len(line)).Int("length", len(record)).Msg("DONE")

		currentOffsetPos += len(line)
		if currentOffsetPos-1 > offsetEnd {
			break
		}
	}

	fmt.Println("Counter: ", counter)
}

func readLine(r *bufio.Reader) []byte {
	line, err := r.ReadSlice('\n')
	if err == nil {
		return line
	}

	if err == io.EOF {
		if len(line) != 0 {
			return line
		}
	} else {
		panic(err)
	}

	return nil
}
