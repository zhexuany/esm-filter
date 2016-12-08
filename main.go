package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/zhexuany/esm-filter/run"
)

// These variables are populated via the Go linker.
var (
	version string
	commit  string
	branch  string
)

func init() {
	// If commit, branch, or build time are not set, make that clear.
	if version == "" {
		version = "unknown"
	}
	if commit == "" {
		commit = "unknown"
	}
	if branch == "" {
		branch = "unknown"
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	m := NewMain()
	if err := m.Run(os.Args[1:]...); err != nil {
		fmt.Fprintf(os.Stderr, "failed to Run ", err)
		os.Exit(1)
	}
}

type Main struct {
	Logger *log.Logger

	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

func NewMain() *Main {
	return &Main{
		Logger: log.New(os.Stderr, "[run]", log.LstdFlags),
		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

// ParseCommandName extracts the command name and args from the args list.
func ParseCommandName(args []string) (string, []string) {
	// Retrieve command name as first argument.
	var name string
	if len(args) > 0 {
		if !strings.HasPrefix(args[0], "-") {
			name = args[0]
		} else if args[0] == "-h" || args[0] == "-help" || args[0] == "--help" {
			// Special case -h immediately following binary name
			name = "help"
		}
	}

	// If command is "help" and has an argument then rewrite args to use "-h".
	if name == "help" && len(args) > 2 && !strings.HasPrefix(args[1], "-") {
		return args[1], []string{"-h"}
	}

	// If a named command is specified then return it with its arguments.
	if name != "" {
		return name, args[1:]
	}
	return "", args
}

func (m *Main) Run(args ...string) error {
	name, args := ParseCommandName(args)
	cmd := run.NewCommand()
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	m.Logger.Println("Listening for signals")
	switch name {
	case "", "run":
		// Block until one of the signals above is received
		select {
		case <-signalCh:
			m.Logger.Println("Signal received, initializing clean shutdown...")
			go func() {
				cmd.Close()
			}()
		}

		// Block again until another signal is received, a shutdown timeout elapses,
		// or the Command is gracefully closed
		m.Logger.Println("Waiting for clean shutdown...")
		select {
		case <-signalCh:
			m.Logger.Println("second signal received, initializing hard shutdown")
		case <-time.After(time.Second * 30):
			m.Logger.Println("time limit reached, initializing hard shutdown")
			// case <-cmd.Closed:
			// m.Logger.Println("server shutdown completed")
		}

	case "config":
		if err := NewVersionCommand().Run(args...); err != nil {
			return fmt.Errorf("version: %s", err)
		}
	default:
		return fmt.Errorf(`unknown command "%s"`+"\n"+`Run esm-filter help for usgae`+"\n\n", name)
	}

	return nil
}

type VersionCommand struct {
	Stdout io.Writer
	Stderr io.Writer
}

func NewVersionCommand() *VersionCommand {
	return &VersionCommand{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
	}
}

func (cmd *VersionCommand) Run(args ...string) error {
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.Usage = func() { fmt.Fprintln(cmd.Stderr, versionUsage) }
	if err := fs.Parse(args); err != nil {
		return err
	}

	// fmt.Fprintln(cmd.Std, "esm-filter v%s (git : %s %s)", version, branch, commit)
	return nil
}

var versionUsage = `Display the esm-filter version, build branch and git commit hash.

Usgae: esm-filter version
`
