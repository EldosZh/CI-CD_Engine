//go:build windows

package runner

import (
	"os/exec"
	"strconv"
	"syscall"
)

func configure(c *exec.Cmd) {
	c.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	c.Cancel = func() error {
		if c.Process == nil {
			return nil
		}
		_ = exec.Command("taskkill", "/F", "/T", "/PID", strconv.Itoa(c.Process.Pid)).Run()
		return c.Process.Kill()
	}
}
