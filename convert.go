package minio_csv_to_json

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/rs/zerolog"
)

const (
	chanSize                   = 256
	minBytesToReadByAGoroutine = 4 * 128
)

var (
	ErrEmptyCSVFile = errors.New("file CSV is empty")
)

type ReadChan <-chan []string

type ErrsChan <-chan error

type convert struct {
	ctx              context.Context
	log              zerolog.Logger
	sheetOption      SheetOption
	minio            *minio.Client
	bucket           string
	fileCsv          string
	fileJson         string
	maxGoroutineUses int
	hasHeaderFile    bool
	bufferSize       int
	lazyQuotes       bool
	columnDelimiter  rune
}

type Convert interface {
	SetHasHeaderFile(hasHeader bool)
	SetBufferSize(size int)
	SetLazyQuotes(lazy bool)
	SetColumnDelimiter(delimiter rune)
	Convert() error
}

type ConvertOption struct {
	Bucket   string
	FileCsv  string
	FileJson string
}

func NewConvert(ctx context.Context, client *minio.Client, sheetOption SheetOption, convertOption ConvertOption) Convert {
	return &convert{
		ctx:              ctx,
		log:              zerolog.New(zerolog.NewConsoleWriter()).With().Ctx(ctx).Caller().Logger(),
		sheetOption:      sheetOption,
		minio:            client,
		bucket:           convertOption.Bucket,
		fileCsv:          convertOption.FileCsv,
		fileJson:         convertOption.FileJson,
		maxGoroutineUses: runtime.NumCPU(),
		hasHeaderFile:    false,
		bufferSize:       4 * 1024,
		lazyQuotes:       false,
		columnDelimiter:  ',',
	}
}

func (c *convert) SetHasHeaderFile(hasHeader bool) {
	c.hasHeaderFile = hasHeader
}

func (c *convert) SetBufferSize(size int) {
	c.bufferSize = size
}

func (c *convert) SetLazyQuotes(lazy bool) {
	c.lazyQuotes = lazy
}

func (c *convert) SetColumnDelimiter(delimiter rune) {
	c.columnDelimiter = delimiter
}

func (c *convert) getCsvContent(bucketName, filePath string) (*minio.Object, error) {
	return c.minio.GetObject(c.ctx, bucketName, filePath, minio.GetObjectOptions{})
}

func (c *convert) Convert() error {
	//! TODO if possible change to readBetweenOffsetsAsync function
	object, err := c.getCsvContent(c.bucket, c.fileCsv)
	if err != nil {
		return err
	}

	defer func(object *minio.Object) {
		err := object.Close()
		if err != nil {
			c.log.Error().Err(err).Msg("failed to close object minio")
		}
	}(object)

	var wg sync.WaitGroup

	temp, err := os.CreateTemp("", uuid.New().String())
	if err != nil {
		return err
	}

	defer func(temp *os.File) {
		err := temp.Close()
		if err != nil {
			c.log.Error().Err(err).Msg("failed to close temp file")
		}
	}(temp)

	defer func(name string) {
		err := os.Remove(name)
		if err != nil {
			c.log.Error().Err(err).Msg("failed to remove temp file")
		}
	}(temp.Name())

	_, err = temp.Write([]byte("["))
	if err != nil {
		return err
	}

	var mutex sync.Mutex
	readChannels, errsChannel := c.read(object)
	for i := 0; i < len(readChannels); i++ {
		wg.Add(1)
		go c.worker(readChannels[i], temp, &wg, &mutex)
	}

	wg.Add(1)

	go func() {
		defer wg.Done()
		for err := range errsChannel {
			fmt.Println("Err: ", err)
		}
	}()

	wg.Wait()

	_, err = temp.Write([]byte("]"))
	if err != nil {
		return err
	}

	_, err = temp.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	_, err = c.minio.PutObject(c.ctx, c.bucket, c.fileJson, temp, -1, minio.PutObjectOptions{
		ContentType: "application/json",
	})
	if err != nil {
		return err
	}

	return nil
}

func (c *convert) worker(readChannels <-chan []string, baseTemp *os.File, wg *sync.WaitGroup, mutex *sync.Mutex) {
	defer wg.Done()

	temp, err := os.CreateTemp("", uuid.New().String())
	if err != nil {
		c.log.Error().Err(err).Msg("failed to create temp file")
	}

	defer func(temp *os.File) {
		err := temp.Close()
		if err != nil {
			c.log.Error().Err(err).Msg("failed to close temp file")
		}
	}(temp)

	defer func(name string) {
		err := os.Remove(name)
		if err != nil {
			c.log.Error().Err(err).Msg("failed to remove temp file")
		}
	}(temp.Name())

	isFirst := true
	for rows := range readChannels {
		value, err := c.sheetOption.ValidateRow(rows)
		if err != nil {
			c.log.Error().Err(err).Msg("failed to validate row")
			continue
		}

		if isFirst {
			isFirst = false
		} else {
			_, err = temp.Write([]byte(","))
			if err != nil {
				c.log.Error().Err(err).Msg("failed to write comma")
			}
		}

		_, err = temp.Write(value)
		if err != nil {
			c.log.Error().Err(err).Msg("failed to write row")
		}
	}

	_, err = temp.Seek(0, io.SeekStart)
	if err != nil {
		c.log.Error().Err(err).Msg("failed to seek temp file")
	}

	mutex.Lock()
	buffer := make([]byte, 1)
	_, err = baseTemp.Seek(1, io.SeekStart)
	if err != nil {
		c.log.Error().Err(err).Msg("failed to seek base temp file")
	}

	_, _ = baseTemp.Read(buffer)

	if string(buffer) != "" {
		_, err = baseTemp.WriteString(",")
		if err != nil {
			c.log.Error().Err(err).Msg("failed to write open bracket")
		}
	}

	_, err = io.Copy(baseTemp, temp)
	if err != nil {
		c.log.Error().Err(err).Msg("failed to copy temp file")
	}
	mutex.Unlock()

	c.log.Debug().Msg("finished worker")
}

func (c *convert) read(object *minio.Object) ([]<-chan []string, ErrsChan) {
	errsChan := make(chan error, chanSize)

	size, err := c.getFileSize(object)
	if err != nil {
		errsChan <- fmt.Errorf("file size error (%w)", err)
		close(errsChan)
		c.log.Error().Err(err).Msg("failed to get file size")
		return nil, errsChan
	}

	threadsInfo := c.computeGoroutineOffsets(size, c.maxGoroutineUses, minBytesToReadByAGoroutine)
	totalThreads := len(threadsInfo)

	c.log.Debug().Int("fileSize", size).Int("totalThreads", totalThreads).Any("threadsInfo", threadsInfo).Msg("stats")

	senderChannels, receiveChannels := createChannel[[]string](totalThreads)
	go c.readAsync(object, threadsInfo, senderChannels, errsChan)

	return receiveChannels, errsChan
}

func (c *convert) readAsync(object *minio.Object, threadsInfo [][2]int, rowsChannels []chan<- []string, errsChan chan<- error) {
	defer func() {
		close(errsChan)
		for i := 0; i < len(rowsChannels); i++ {
			close(rowsChannels[i])
		}
	}()

	totalThreads := len(threadsInfo)

	var wg sync.WaitGroup
	wg.Add(totalThreads)
	for thread := 0; thread < totalThreads; thread++ {
		go c.readBetweenOffsetsAsync(
			object,
			thread+1,
			threadsInfo[thread][0],
			threadsInfo[thread][1],
			&wg,
			rowsChannels[thread],
			errsChan,
		)
	}

	wg.Wait()

	c.log.Debug().Msg("finished reading file")
}

func (c *convert) readBetweenOffsetsAsync(object *minio.Object, currentThreadNo, offsetStart, offsetEnd int, wg *sync.WaitGroup, rowsChan chan<- []string, errsChan chan<- error) {
	defer wg.Done()

	var line []byte
	r := bufio.NewReaderSize(object, c.bufferSize)
	_, _ = object.Seek(int64(offsetStart), io.SeekStart)
	if currentThreadNo != 1 || c.hasHeaderFile {
		line = c.readLine(r, currentThreadNo, offsetStart, errsChan)

		if line == nil {
			return
		}
	}
	realOffsetStart := offsetStart + len(line)
	currentOffsetPos := realOffsetStart

	bytesReader := bytes.NewReader(line)
	csvReader := csv.NewReader(bytesReader)
	csvReader.Comma = c.columnDelimiter
	csvReader.FieldsPerRecord = c.sheetOption.GetColumnLength()
	csvReader.LazyQuotes = c.lazyQuotes

ForLoop:
	for {
		select {
		case <-c.ctx.Done():
			if c.ctx.Err() != nil {
				errsChan <- fmt.Errorf(
					"thread #%d received context error (%w)",
					currentThreadNo, c.ctx.Err(),
				)
			}

			return
		default:
			line = c.readLine(r, currentThreadNo, currentOffsetPos, errsChan)
			if line == nil {
				break ForLoop
			}

			bytesReader.Reset(line)
			record, err := csvReader.Read()
			if err != nil {
				errsChan <- fmt.Errorf(
					"thread #%d could not parse row at offset %d (%w)",
					currentThreadNo, currentOffsetPos, err,
				)

				c.log.Error().Err(err).Int("thread", currentThreadNo).Int("offset", currentOffsetPos).Msg("could not parse row")
			} else {
				rowsChan <- record
			}

			currentOffsetPos += len(line)
			if currentOffsetPos-1 > offsetEnd {
				break ForLoop
			}
		}
	}

	c.log.Debug().Int("thread", currentThreadNo).Int("OffsetStart", offsetStart).Int("OffsetEnd", offsetEnd).Int("RealOffsetStart", realOffsetStart).Int("RealOffsetEnd", currentOffsetPos+1).Int("BytesCount", currentOffsetPos+realOffsetStart).Msg("DONE")
}

func (c *convert) readLine(r *bufio.Reader, thread, offsetPos int, errsChan chan<- error) []byte {
	line, err := r.ReadSlice('\n')
	if err == nil {
		return line
	}

	if err == io.EOF {
		if len(line) != 0 {
			return line
		}
	} else {
		errsChan <- fmt.Errorf(
			"thread #%d could not read line at offset %d (%w)",
			thread, offsetPos, err,
		)

		c.log.Error().Err(err).Int("thread", thread).Int("offset", offsetPos).Msg("could not read line")
	}

	return nil
}

func (c *convert) computeGoroutineOffsets(totalBytes, maxGoroutines, minBytesReadByAGoroutine int) [][2]int {
	if totalBytes <= 0 {
		return nil
	}

	if minBytesReadByAGoroutine <= 0 {
		minBytesReadByAGoroutine = 1
	}

	if maxGoroutines <= 0 {
		maxGoroutines = 1
	}

	if totalBytes <= minBytesReadByAGoroutine {
		return [][2]int{{0, totalBytes - 1}}
	}

	totalGoroutines := totalBytes / minBytesReadByAGoroutine
	if totalGoroutines == 1 {
		return [][2]int{{0, totalBytes - 1}}
	}

	if totalGoroutines > maxGoroutines {
		totalGoroutines = maxGoroutines
	}

	bytesPerGoroutine := totalBytes / totalGoroutines
	distribution := make([][2]int, totalGoroutines)
	start, end := 0, bytesPerGoroutine-1
	for goroutineNo := 0; goroutineNo < totalGoroutines-1; goroutineNo++ {
		distribution[goroutineNo] = [2]int{start, end}
		start = end + 1
		end += bytesPerGoroutine
	}
	distribution[totalGoroutines-1] = [2]int{start, totalBytes - 1}
	return distribution
}

func (c *convert) getFileSize(object *minio.Object) (int, error) {
	stat, err := object.Stat()
	if err != nil {
		return 0, err
	}

	fileSize := int(stat.Size)
	if fileSize < 1 {
		return 0, ErrEmptyCSVFile
	}

	return fileSize, nil
}

func createChannel[T any](long int) ([]chan<- T, []<-chan T) {
	sendChannels := make([]chan<- T, long)
	receiveChannels := make([]<-chan T, long)
	for i := 0; i < long; i++ {
		channel := make(chan T, chanSize)
		sendChannels[i] = channel
		receiveChannels[i] = channel
	}
	return sendChannels, receiveChannels
}
