package glusterfs

// glusterfs.go

import (
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/inputs"

	"bufio"
	"bytes"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type runner func(cmdName string, Volume string, Timeout internal.Duration, UseSudo bool) (*bytes.Buffer, error)

var defaultTimeout = internal.Duration{Duration: time.Second}
var defaultBinary = "/usr/sbin/gluster"
var defaultVolumes = []string{"vol0"}

var matchBrick = regexp.MustCompile("^Brick: (.*)$")
var matchRead = regexp.MustCompile("Data Read: ([0-9]+) bytes$")
var matchWrite = regexp.MustCompile("Data Written: ([0-9]+) bytes$")
var matchFop = regexp.MustCompile("^[0-9]+.[0-9]+")

type GlusterFS struct {
	run     runner
	Volumes []string
	Binary  string
	Timeout internal.Duration
	UseSudo bool
}

var sampleConfig = `
## If running as a restricted user you can prepend sudo for additional access:
#use_sudo = false

## The default location of the smtpctl binary can be overridden with:
binary = "/usr/sbin/gluster"

# The default timeout of 1000ms can be overriden with (in milliseconds):
timeout = 1000

## By default, telegraf gather stats for all numerical metric points.
## Setting stats will override the defaults shown below.
## Glob matching can be used, ie, stats = ["mda.*", "mta.*"]
volumes = ["vol0"]
`

func (gfs *GlusterFS) Description() string {
	return "Plugin reading values from the GlusterFS profiler"
}

func (gfs *GlusterFS) SampleConfig() string {
	return sampleConfig
}

// Shell out to opensmtpd_stat and return the output
func glusterfsRunner(cmdName string, Volume string, Timeout internal.Duration, UseSudo bool) (*bytes.Buffer, error) {
	var out bytes.Buffer
	var cmdArgs = []string{"volume", "profile", Volume, "info", "cumulative"}

	cmd := exec.Command(cmdName, cmdArgs...)

	if UseSudo {
		cmdArgs = append([]string{cmdName}, cmdArgs...)
		cmd = exec.Command("sudo", cmdArgs...)
	}

	cmd.Stdout = &out
	err := internal.RunTimeout(cmd, Timeout.Duration)
	if err != nil {
		return &out, fmt.Errorf("error running gluster command: %s - use_sudo: %t - cmdArgs: %v", err, UseSudo, cmdArgs)
	}

	return &out, nil
}

func (gfs *GlusterFS) Gather(acc telegraf.Accumulator) error {
	for _, volume := range gfs.Volumes {

		// 		var cmdArgs = []string{"volume", "profile", volume, "info", "cumulative"}
		//
		// 		cmd := exec.Command(cmdName, cmdArgs...)
		//
		// 		if gfs.UseSudo {
		// 			cmdArgs = append([]string{cmdName}, cmdArgs...)
		// 			cmd = exec.Command("sudo", cmdArgs...)
		// 		}
		//
		// 		var out bytes.Buffer
		// 		cmd.Stdout = &out
		// 		err := internal.RunTimeout(cmd, defaultTimeout.Duration)
		// 		if err != nil {
		// 			return fmt.Errorf("error running gluster command: %s - use_sudo: %t - cmdArgs: %v", err, gfs.UseSudo, cmdArgs)
		//

		out, err := gfs.run(gfs.Binary, volume, gfs.Timeout, gfs.UseSudo)
		if err != nil {
			return fmt.Errorf("error gathering metrics: %s", err)
		}

		scanner := bufio.NewScanner(out)

		var tags map[string]string

		for scanner.Scan() {
			var txt = scanner.Text()

			fmt.Printf("%s", txt)
			if brick := matchBrick.FindStringSubmatch(txt); brick != nil {
				tags = map[string]string{"volume": volume, "brick": brick[1]}
			} else if gread := matchRead.FindStringSubmatch(txt); gread != nil {
				var val, _ = strconv.Atoi(gread[1])
				acc.AddFields("glusterfs", map[string]interface{}{"read": val}, tags)
			} else if gwrite := matchWrite.FindStringSubmatch(txt); gwrite != nil {
				var val, _ = strconv.Atoi(gwrite[1])
				acc.AddFields("glusterfs", map[string]interface{}{"write": val}, tags)
			} else if matchFop.MatchString(strings.TrimSpace(txt)) {
				fields := strings.Fields(strings.TrimSpace(txt))
				fmt.Printf("match: %v\n", fields)
				for index, element := range fields {
					fmt.Printf("%d %s\n", index, element)
				}

				if len(fields) == 9 {

					fop_line := make(map[string]interface{})
					fop_name := strings.ToLower(fields[8])

					fop_line[fop_name+"_pct_latency"], err = strconv.ParseFloat(fields[0], 64)
					if err != nil {
						acc.AddError(fmt.Errorf("Expected a numerical value for %s = %v\n",
							"pct_latency", fields[0]))
					}
					fop_line[fop_name+"_avg_latency"], err = strconv.ParseFloat(fields[1], 64)
					if err != nil {
						acc.AddError(fmt.Errorf("Expected a numerical value for %s = %v\n",
							"avg_latency", fields[1]))
					}
					fop_line[fop_name+"_min_latency"], err = strconv.ParseFloat(fields[3], 64)
					if err != nil {
						acc.AddError(fmt.Errorf("Expected a numerical value for %s = %v\n",
							"min_latency", fields[3]))
					}
					fop_line[fop_name+"_max_latency"], err = strconv.ParseFloat(fields[5], 64)
					if err != nil {
						acc.AddError(fmt.Errorf("Expected a numerical value for %s = %v\n",
							"max_latency", fields[5]))
					}
					fop_line[fop_name+"_ncalls"], err = strconv.ParseFloat(fields[7], 64)
					if err != nil {
						acc.AddError(fmt.Errorf("Expected a numerical value for %s = %v\n",
							"ncalls", fields[7]))
					}
					fmt.Printf("%v\n", fop_line)
					acc.AddFields("glusterfs", fop_line, tags)
				}
			}
		}
	}
	return nil
}

func init() {
	inputs.Add("glusterfs", func() telegraf.Input {
		return &GlusterFS{
			run:     glusterfsRunner,
			Volumes: defaultVolumes,
			Binary:  defaultBinary,
			Timeout: defaultTimeout,
			UseSudo: false,
		}
	})
}
