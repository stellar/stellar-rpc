package bench

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"

	"github.com/spf13/cobra"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"
)

// profileFlags is the optional Go-profiling flag pair shared by both
// subcommands.
type profileFlags struct {
	cpuProfile string
	memProfile string
}

func (p *profileFlags) bind(cmd *cobra.Command) {
	fs := cmd.Flags()
	fs.StringVar(&p.cpuProfile, "cpuprofile", "", "write a Go CPU profile to PATH")
	fs.StringVar(&p.memProfile, "memprofile", "", "write a Go heap profile to PATH (taken at run end)")
}

// around runs fn under the configured profiles: the CPU profile spans fn, and
// the heap profile is written after fn returns (before stores close).
func (p *profileFlags) around(logger *supportlog.Entry, fn func() error) error {
	if p.cpuProfile != "" {
		f, err := os.Create(p.cpuProfile)
		if err != nil {
			return fmt.Errorf("create %s: %w", p.cpuProfile, err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			_ = f.Close()
			return fmt.Errorf("start CPU profile: %w", err)
		}
		defer func() {
			pprof.StopCPUProfile()
			_ = f.Close()
			logger.Infof("wrote CPU profile to %s", p.cpuProfile)
		}()
	}
	err := fn()
	if p.memProfile != "" {
		p.writeHeapProfile(logger)
	}
	return err
}

func (p *profileFlags) writeHeapProfile(logger *supportlog.Entry) {
	f, err := os.Create(p.memProfile)
	if err != nil {
		logger.Errorf("create %s: %v", p.memProfile, err)
		return
	}
	defer func() { _ = f.Close() }()
	runtime.GC()
	if err := pprof.WriteHeapProfile(f); err != nil {
		logger.Errorf("write heap profile: %v", err)
		return
	}
	logger.Infof("wrote heap profile to %s", p.memProfile)
}
