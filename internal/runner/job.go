package runner

import (
	"fmt"
	"github.com/flosch/pongo2/v5"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"
)

type Job struct {
	line    int64
	workdir string
	context pongo2.Context
	tpl     *pongo2.Template
}

type JobResult struct {
	Elapsed int64
	Success bool
	Context pongo2.Context
	Output  string
}

func (j *Job) Exec() JobResult {
	jobStart := time.Now().Unix()
	scriptPath := fmt.Sprintf("%v/script.sh", j.workdir)
	// Write tpl to file
	script, err := j.tpl.Execute(j.context)
	if err != nil {
		log.Printf("Cannot parse template at line %v, %v\n", j.line, err)
		return JobResult{
			Elapsed: time.Now().Unix() - jobStart,
			Success: false,
			Output:  err.Error(),
			Context: j.context,
		}
	}
	f, err := os.OpenFile(scriptPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0700)
	if err == nil {
		_, err = f.WriteString(script)
	}
	if err != nil {
		log.Printf("Cannot write script at line %v, %v\n", j.line, err)
		return JobResult{
			Elapsed: time.Now().Unix() - jobStart,
			Success: false,
			Output:  err.Error(),
			Context: j.context,
		}
	}
	// Exec
	buf, err := exec.Command("/bin/bash", scriptPath).CombinedOutput()
	output := strings.Trim(string(buf), "\n ")
	if err != nil {
		log.Printf("Exec failed at line %v: %v\n", j.line, output)
		return JobResult{
			Elapsed: time.Now().Unix() - jobStart,
			Success: false,
			Output:  output,
			Context: j.context,
		}
	}
	// All good
	return JobResult{
		Elapsed: time.Now().Unix() - jobStart,
		Success: true,
		Output:  output,
		Context: j.context,
	}
}
