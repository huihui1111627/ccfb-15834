package selftest

import (
	"testing"
	"time"
)

func TestDeterministicMultipathConsistency(t *testing.T) {
	report, mgr, _ := Run(Config{
		Seed:     20261127,
		Disks:    48,
		Duration: 5 * time.Second,
	})
	if mgr != nil {
		defer mgr.Close()
	}
	if !report.Pass {
		for _, failure := range report.Failures {
			t.Error(failure)
		}
		t.Fatalf("multipath self-test failed: %+v", report)
	}
	t.Logf("ops=%d blocks=%d suspensions=%d maxSuspend=%dms maxRequest=%dms",
		report.Operations, report.VerifiedBlocks, report.Manager.Summary.TotalSuspensions,
		report.Manager.Summary.MaxSuspensionWaitMS, report.MaxRequestWaitMS)
}
