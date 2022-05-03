package cmd

import (
	"fmt"
	"github.com/johnuopini/csvexec/internal/display"
	"github.com/johnuopini/csvexec/internal/runner"
	"github.com/johnuopini/csvexec/internal/utils"
	"github.com/spf13/cobra"
	"log"
	"os"
)

var rootCmd = &cobra.Command{
	Use:   "csvexec csv_file template_file",
	Short: "Execute a given command for every CSV line in parallel",
	Args:  cobra.MinimumNArgs(2),
	Long: `This tool allows splitting a CSV job into multiple file using a template
engine to create execution scripts, tool will collect success/errors and write a 
resulting CSV files with failed entries`,
	Run: func(cmd *cobra.Command, args []string) {
		csvFile := args[0]
		if !utils.Exists(csvFile) {
			panic(fmt.Errorf("file %v does not exist", csvFile))
		}
		tplFile := args[1]
		if !utils.Exists(tplFile) {
			panic(fmt.Errorf("file %v does not exist", csvFile))
		}
		workdir, _ := cmd.Flags().GetString("workdir")
		if !utils.Exists(workdir) {
			err := os.Mkdir(workdir, 0700)
			if err != nil {
				panic(err)
			}
		}
		initLogging(workdir)
		// Get jobs and start runner
		jobs, _ := cmd.Flags().GetUint32("jobs")
		runner, err := runner.NewCsvRunner(csvFile, tplFile, workdir, jobs)
		if err != nil {
			panic(err)
		}
		// Stop at
		stopAt, _ := cmd.Flags().GetUint64("limit")
		if stopAt == 0 {
			stopAt = uint64(runner.Lines)
		}
		// Init progress
		progress := display.New(int(stopAt), &runner)
		progress.PrintHeader()
		// Start
		status, err := runner.Process(stopAt, progress.Callback)
		if err != nil {
			panic(err)
		}
		progress.PrintFooter(status)
	},
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func initLogging(path string) {
	f, err := os.OpenFile(fmt.Sprintf("%v/log.txt", path), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		panic(err)
	}
	// Set output of logs to f
	log.SetOutput(f)
	log.Printf("Log started")
}

func init() {
	rootCmd.PersistentFlags().Uint32("jobs", 8, "number of jobs to execute in parallel")
	rootCmd.PersistentFlags().Uint64("limit", 0, "stop after executing limit jobs")
	rootCmd.PersistentFlags().String("workdir", "/tmp/csvexec", "job working directory")
}
