package runner

import (
	"encoding/csv"
	"fmt"
	"github.com/flosch/pongo2/v5"
	"github.com/johnuopini/csvexec/internal/utils"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

type CsvRunner struct {
	CsvFile  string
	TplFile  string
	WorkPath string
	Jobs     int
	Lines    int
	tpl      *pongo2.Template
}

type Status struct {
	Parsed  int
	Total   int
	Errors  int
	Elapsed time.Duration
}

type JobRequest struct {
	line int64
	data []string
}

func NewCsvRunner(
	csvFile string,
	tplFile string,
	workPath string,
	jobs uint32,
) (CsvRunner, error) {
	// Compute lines and check CSV
	file, err := os.Open(csvFile)
	if err != nil {
		return CsvRunner{}, err
	}
	lines, err := utils.LineCount(file)
	if err != nil {
		return CsvRunner{}, err
	}
	_ = file.Close()
	// Load template
	tpl, err := pongo2.FromFile(tplFile)
	if err != nil {
		panic(err)
	}
	// Done
	return CsvRunner{
		CsvFile:  csvFile,
		TplFile:  tplFile,
		WorkPath: workPath,
		Jobs:     int(jobs),
		Lines:    int(lines) - 1,
		tpl:      tpl,
	}, nil
}

//goland:noinspection GoUnhandledErrorResult
func (cr *CsvRunner) Process(stopAt uint64, callback func(status Status)) (Status, error) {
	start := time.Now().Unix()
	file, err := os.Open(cr.CsvFile)
	if err != nil {
		return Status{}, err
	}
	reader := csv.NewReader(file)
	reader.LazyQuotes = true
	header, err := reader.Read()
	if err != nil {
		return Status{}, err
	}
	// Main
	jobs := utils.Min(int(stopAt), cr.Jobs)
	lineCount := utils.Min(int(stopAt), cr.Lines)
	var lineParsed uint64
	var lineFailed uint64
	var chIn = make(chan JobRequest, lineCount)
	var chOut = make(chan JobResult)
	var chEnd = make(chan int, 1)
	var wg sync.WaitGroup
	wg.Add(jobs)
	for i := 0; i < jobs; i++ {
		jobIndex := i
		go func() {
			for {
				request, ok := <-chIn
				// Nothing to do, channel has been closed, end the goroutine
				if !ok {
					chEnd <- jobIndex
					wg.Done()
					return
				}
				// Prepare workdir
				workdir := fmt.Sprintf("%v/%06d", cr.WorkPath, jobIndex)
				if !utils.Exists(workdir) {
					err := os.Mkdir(workdir, 0700)
					if err != nil {
						panic(err)
					}
				}
				// Create job map
				var context = pongo2.Context{}
				for index, entry := range request.data {
					key := strings.Trim(strings.ToLower(header[index]), " \"")
					value := strings.Trim(entry, " \"")
					context[key] = value
				}
				job := Job{
					line:    request.line,
					workdir: workdir,
					context: context,
					tpl:     cr.tpl,
				}
				result := job.Exec()
				chOut <- result
				// Cleanup
				_ = os.RemoveAll(workdir)
			}
		}()
	}
	// Status update and log writer
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Open failure CSV
		f, err := os.OpenFile(
			fmt.Sprintf("%v/failures.csv", cr.WorkPath),
			os.O_WRONLY|os.O_CREATE|os.O_TRUNC,
			0600,
		)
		if err != nil {
			log.Printf("Unable to create CSV for failures, %v", err)
			panic(err)
		}
		defer f.Close()
		failures := csv.NewWriter(f)
		ended := 0
		for {
			select {
			case result := <-chOut:
				lineParsed = lineParsed + 1
				if !result.Success {
					lineFailed = lineFailed + 1
					var arr []string
					for _, v := range result.Context {
						arr = append(arr, fmt.Sprintf("%v", v))
					}
					arr = append(arr, fmt.Sprintf("%v", result.Output))
					err = failures.Write(arr)
					failures.Flush()
					if err != nil {
						log.Printf("Unable to write CSV for failures, %v", err)
					}
				}
				callback(Status{
					Parsed:  int(lineParsed),
					Total:   lineCount,
					Errors:  int(lineFailed),
					Elapsed: time.Second * time.Duration(time.Now().Unix()-start),
				})
			case <-chEnd:
				ended += 1
				if ended == jobs {
					return
				}
			}
		}
	}()
	// Read from CSV and add to channel
	for i := 0; i < lineCount; i++ {
		line, err := reader.Read()
		if err != nil {
			log.Printf("Output reading line from CSV at %v\n", i)
		}
		chIn <- JobRequest{
			line: int64(i),
			data: line,
		}
	}
	// Close channel and wait
	close(chIn)
	wg.Wait()
	return Status{
		Parsed:  int(lineParsed),
		Total:   lineCount,
		Errors:  int(lineFailed),
		Elapsed: time.Second * time.Duration(time.Now().Unix()-start),
	}, nil
}
