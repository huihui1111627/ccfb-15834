package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"southpole/mpio/internal/selftest"
)

func main() {
	seed := flag.Int64("seed", 20261127, "fixed random seed")
	disks := flag.Int("disks", 48, "number of simulated disks")
	duration := flag.Duration("duration", 7*time.Second, "mixed IO/fault injection duration")
	jsonOut := flag.String("json", "selftest-report.json", "write JSON report to this path (empty disables)")
	csvOut := flag.String("csv", "hourly-stats.csv", "write hourly CSV stats to this path (empty disables)")
	flag.Parse()

	cfg := selftest.Config{Seed: *seed, Disks: *disks, Duration: *duration}
	report, mgr, _ := selftest.Run(cfg)

	fmt.Printf("PASS=%v disks=%d ops=%d reads=%d writes=%d bytes=%d verified4K=%d mismatches=%d ioErrors=%d\n",
		report.Pass, report.DiskCount, report.Operations, report.ReadOps, report.WriteOps, report.Bytes,
		report.VerifiedBlocks, report.Mismatches, report.IOErrors)
	s := report.Manager.Summary
	fmt.Printf("paths up/check/down=%d/%d/%d suspensions=%d maxSuspend=%dms totalSuspendWait=%dms maxRequest=%dms\n",
		s.PathsUp, s.PathsChecking, s.PathsDown, s.TotalSuspensions, s.MaxSuspensionWaitMS,
		s.TotalSuspensionWaitMS, report.MaxRequestWaitMS)
	fmt.Printf("events=%d hourlyRows=%d elapsed=%dms\n", len(report.Manager.Events), report.HourlyRows, report.ElapsedMS)
	if len(report.Failures) > 0 {
		fmt.Println("failures:")
		for _, f := range report.Failures {
			fmt.Println(" -", f)
		}
	}

	if *jsonOut != "" {
		data, err := json.MarshalIndent(report, "", "  ")
		if err == nil {
			_ = os.WriteFile(*jsonOut, data, 0o644)
		}
	}
	if *csvOut != "" && mgr != nil {
		data, err := mgr.ExportHourlyCSV("")
		if err == nil {
			_ = os.WriteFile(*csvOut, data, 0o644)
		}
	}
	if mgr != nil {
		_ = mgr.Close()
	}
	if !report.Pass {
		os.Exit(1)
	}
}
