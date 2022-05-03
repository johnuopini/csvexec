package display

import (
	"fmt"
	"github.com/johnuopini/csvexec/internal/runner"
	"github.com/vbauerster/mpb/v7"
	"github.com/vbauerster/mpb/v7/decor"
	"time"
)

type Progress struct {
	stopAt int
	runner *runner.CsvRunner
	bar    *mpb.Bar
}

func New(stopAt int, runner *runner.CsvRunner) Progress {
	p := mpb.New(mpb.WithWidth(64))
	start := time.Now().UnixMilli()
	total := runner.Lines
	name := " - Eta: "
	// create a single bar, which will inherit container's width
	bar := p.New(int64(total),
		// BarFillerBuilder with custom style
		//mpb.BarStyle().Lbound("╢").Filler("▌").Tip("▌").Padding("░").Rbound("╟"),
		mpb.BarStyle(),
		mpb.PrependDecorators(
			// display our name with one space on the right
			decor.Name(name),
			// replace ETA decorator with "done" message, OnComplete event
			decor.OnComplete(
				decor.AverageETA(decor.ET_STYLE_GO), "done",
			),
		),
		mpb.AppendDecorators(
			decor.NewPercentage("%.1f"),
			// We use refill ass errors
			decor.Any(func(statistics decor.Statistics) string {
				avg := time.Millisecond * time.Duration(
					int(float64(time.Now().UnixMilli()-start)/float64(statistics.Current)),
				)
				return fmt.Sprintf(" [failed:%v] [avg:%s]", statistics.Refill, avg)
			})),
	)
	return Progress{
		stopAt: stopAt,
		runner: runner,
		bar:    bar,
	}
}

func (p *Progress) PrintHeader() {
	fmt.Printf(
		"Starting runner %v:\n"+
			" - Jobs: %v, total %v lines, stop at %v\n"+
			" - Csv: %v\n"+
			" - Template: %v\n"+
			" - Workdir: %v\n",
		time.Now().Format(time.Layout),
		p.runner.Jobs,
		p.runner.Lines,
		p.stopAt,
		p.runner.CsvFile,
		p.runner.TplFile,
		p.runner.WorkPath,
	)
}

func (p *Progress) PrintFooter(status runner.Status) {
	p.bar.SetCurrent(int64(status.Parsed))
	fmt.Printf(
		" - Completed %v jobs out of %v with %v errors\n - Elapsed: %s\n",
		status.Parsed,
		status.Total,
		status.Errors,
		status.Elapsed,
	)
}

func (p *Progress) Callback(status runner.Status) {
	p.bar.SetCurrent(int64(status.Parsed))
	p.bar.SetRefill(int64(status.Errors))
}
