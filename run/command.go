package run

import (
	"flag"
	"fmt"
	"github.com/zhexuany/esm-filter/client"
	"io"
	"log"
	"os"
	"runtime"
)

const logo = `
8888888888  .d8888b.  888b     d888         .d888 d8b 888 888                    
888        d88P  Y88b 8888b   d8888        d88P"  Y8P 888 888                    
888        Y88b.      88888b.d88888        888        888 888                    
8888888     "Y888b.   888Y88888P888        888888 888 888 888888  .d88b.  888d888
888            "Y88b. 888 Y888P 888        888    888 888 888    d8P  Y8b 888P"  
888              "888 888  Y8P  888 888888 888    888 888 888    88888888 888    
888        Y88b  d88P 888   "   888        888    888 888 Y88b.  Y8b.     888    
8888888888  "Y8888P"  888       888        888    888 888  "Y888  "Y8888  888  
`

type Command struct {
	Version   string
	Branch    string
	Commit    string
	BuildTime string

	closing chan struct{}
	Closed  chan struct{}

	Stdin  io.Writer
	Stdout io.Writer
	Stderr io.Writer

	Server *Server
}

func NewCommand() *Command {
	return &Command{
		closing: make(chan struct{}),
		Closed:  make(chan struct{}),
		Stdin:   os.Stdin,
		Stdout:  os.Stdout,
		Stderr:  os.Stderr,
	}
}

func (cmd *Command) Run(args ...string) error {
	options, err := cmd.ParseFlags(args...)
	if err != nil {
		return nil
	}

	fmt.Print(logo)

	log.SetFlags(log.LstdFlags)

	runtime.GOMAXPROCS(runtime.NumCPU())

	log.Printf("esm-filter starting, version %s, branch %s, commit %s",
		cmd.Version, cmd.Branch, cmd.Commit)
	log.Printf("Go version %s, GOMAXPROCS set to %d", runtime.Version(), runtime.GOMAXPROCS(0))

	// log.Println(options.GetConfigPath())

	config, err := client.ParseConfig(options.GetConfigPath())
	if err != nil {
		return fmt.Errorf("parse config:%s", err)
	}
	// Create a new server
	cmd.Server = NewServer(config)
	if err := cmd.Server.Open(); err != nil {
		log.Fatalf("open server: %s", err)
	}

	log.Printf("Server start to Run")
	go cmd.Server.Run()

	return nil
}

func (cmd *Command) Close() error {
	defer close(cmd.Closed)
	close(cmd.closing)
	if cmd.Server != nil {
		return cmd.Server.Close()
	}
	return nil
}

func (cmd *Command) ParseFlags(args ...string) (Options, error) {
	var opt Options
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.StringVar(&opt.ConfigPath, "config", "", "")
	fs.Usage = func() { fmt.Fprintln(cmd.Stderr, usage) }
	if err := fs.Parse(args); err != nil {
		return Options{}, err
	}

	return opt, nil
}

type Options struct {
	ConfigPath string
}

var usage = `Runs the esm-filter server.

Usgae esm-filter run [flags]

    -config <path>
            Set the path to the configuration file.
            This defaults to the environment variable ESM-FILTER_CONFIG_PATH,
            ~/.esm-filter/esm-filter.conf, or /etc/esm-filter/esm-filter.conf if a file
            is present at any of these locations.
            Disable the automatic loading of a configuration file using
            the null device (such as /dev/null).
`

func (opt *Options) GetConfigPath() string {
	if opt.ConfigPath != "" {
		if opt.ConfigPath == os.DevNull {
			return ""
		}
		return opt.ConfigPath
	} else if envVar := os.Getenv("ESM_Filter_CONFIG_PATH"); envVar != "" {
		return envVar
	}

	for _, path := range []string{
		os.ExpandEnv("${HOME}/.esm-filter/esm-filter.conf"),
		"/etc/esm-filter/esm-filter.conf",
	} {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}
	return ""
}
