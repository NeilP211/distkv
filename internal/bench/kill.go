package bench

import (
	"fmt"
	"os"
)

// killPID sends SIGKILL to the process with the given pid.
func killPID(pid int) error {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return fmt.Errorf("find process %d: %w", pid, err)
	}
	return proc.Kill()
}
