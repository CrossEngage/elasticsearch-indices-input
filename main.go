//go:generate bash ./g_version.sh
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"log/syslog"
	"net/http"
	"net/url"
	"os"
	"path"
	"reflect"
	"strings"
	"time"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	appName      = path.Base(os.Args[0])
	app          = kingpin.New(appName, "A telegraf input plugin that gatters metrics for every ElasticSearch index, by CrossEngage")
	checkName    = app.Flag("name", "Check name").Default(appName).String()
	baseURL      = app.Flag("addr", "The base HTTP URL of the ElasticSearch node").Default("http://localhost:9200").URL()
	debug        = app.Flag("debug", "If set, enables debug logs").Default("false").Bool()
	stderr       = app.Flag("stderr", "If set, enables logging to stderr instead of syslog").Default("false").Bool()
	indicesWcard = app.Flag("indices", "Index name wildcard").Default("*").String()
)

func main() {
	app.Version(version)
	kingpin.MustParse(app.Parse(os.Args[1:]))

	if *debug {
		log.SetFlags(log.LstdFlags | log.Lshortfile)
	}

	if *stderr {
		log.SetOutput(os.Stderr)
	} else {
		slog, err := syslog.New(syslog.LOG_NOTICE|syslog.LOG_DAEMON, appName)
		if err != nil {
			log.Fatal(err)
		}
		log.SetOutput(slog)

	}

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}
	commonKeys := []string{*checkName, "host=" + hostname}

	// TODO timeouts
	tr := &http.Transport{}
	client := &http.Client{Transport: tr}

	loc, err := url.Parse((*baseURL).String() + "/" + *indicesWcard + "/_stats")
	if err != nil {
		log.Fatal(err)
	}

	timestamp := time.Now()
	resp, err := client.Get(loc.String())
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		log.Fatalf("%s %s", loc, resp.Status)
	}

	jsonResp := &jsonResp{}
	if err := json.NewDecoder(resp.Body).Decode(jsonResp); err != nil {
		log.Fatal(err)
	}

	for index, stats := range jsonResp.Indices {
		tags := []string{}
		tags = append(tags, commonKeys...)
		tags = append(tags, fmt.Sprintf("index=%s", index))

		primariesTags := make([]string, len(tags))
		copy(primariesTags, tags)

		totalTags := make([]string, len(tags))
		copy(totalTags, tags)

		primariesTags = append(primariesTags, "shards=primaries")
		primariesValues := flattenValues(stats.Primaries)
		fmt.Println(strings.Join(primariesTags, ","), strings.Join(primariesValues, ","), timestamp.UnixNano())

		totalTags = append(totalTags, "shards=all")
		totalValues := flattenValues(stats.Total)
		fmt.Println(strings.Join(totalTags, ","), strings.Join(totalValues, ","), timestamp.UnixNano())
	}
}

type valueMap map[string]map[string]interface{}

type jsonResp struct {
	// _shards is ignored
	// _all is ignored
	Indices map[string]struct {
		Primaries valueMap `json:"primaries"`
		Total     valueMap `json:"total"`
	} `json:"indices"`
}

func flattenValues(vmap valueMap) []string {
	values := []string{}
	for rootKey, subMap := range vmap {
		for valueKey, value := range subMap {
			rt := reflect.TypeOf(value)
			if rt.Kind() == reflect.Slice {
				continue
			}
			switch v := value.(type) {
			case int64, int32, int16, int8, int, uint64, uint32, uint16, uint8, uint:
				values = append(values, fmt.Sprintf(`%s.%s=%di`, rootKey, valueKey, v))
			case float32, float64, complex64, complex128:
				values = append(values, fmt.Sprintf(`%s.%s=%f`, rootKey, valueKey, v))
			case string:
				values = append(values, fmt.Sprintf(`%s.%s="%s"`, rootKey, valueKey, v))
			}
		}
	}
	return values
}
