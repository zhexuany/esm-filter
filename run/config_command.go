package run

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/zhexuany/esm-filter/client"
)

type PrintConfigCommand struct {
	Stdin io.Reader

	Stdout io.Writer
	Stderr io.Writer
}

func NewPrintConfigCommand() *PrintConfigCommand {
	return &PrintConfigCommand{
		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

func (cmd *PrintConfigCommand) Run(args ...string) error {
	//Parse command flags
	fs := flag.NewFlagSet((""), flag.ContinueOnError)
	configPath := fs.String("config", "", "")
	fs.Usage = func() { fmt.Fprintln(cmd.Stderr, printConfigUsage) }
	if err := fs.Parse(args); err != nil {
		return err
	}

	opt := Options{ConfigPath: *configPath}
	config, err := client.ParseConfig(opt.GetConfigPath())
	if err != nil {
		return fmt.Errorf("parse config: %s", err)
	}

	// Apply any environment variables on top of the parsed config
	if err := config.ApplyEnvOverrides(); err != nil {
		return fmt.Errorf("apply enc config: %v", err)
	}

	//check config is valid or not
	if err := config.Validate(); err != nil {
		return fmt.Errorf("%s. To generate a valid configuration file run `esm-filter config > esm_filter.generated.conf`", err)
	}

	toml.NewEncoder(cmd.Stdout).Encode(config)
	fmt.Fprint(cmd.Stdout, "\n")

	return nil
}

var printConfigUsage = `Displays the default configuration.

Usage: esm-filter config [flags]

    -config <path>
            Set the path to the initial configuration file.
            This defaults to the environment variable ESM_FILTER_CONFIG_PATH,
            ~/.esm-filter/esm-filter.conf, or /etc/esm-filter/esm-filter.conf if a file
            is present at any of these locations.
            Disable the automatic loading of a configuration file using
            the null device (such as /dev/null).
`
