package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/hashicorp/go-retryablehttp"
)

func monitorSite(site SiteCfg) error {

	client := retryablehttp.NewClient()
	client.HTTPClient.Timeout = time.Duration(site.ResponseSeconds) * time.Second
	client.RetryWaitMin = 4 * time.Second
	client.RetryWaitMax = 64 * time.Second
	client.RetryMax = site.Retries

	req, err := retryablehttp.NewRequest(http.MethodGet, site.URL, nil)
	if err != nil {
		return err
	}

	for k, v := range site.Headers {
		req.Header.Add(k, v)
	}

	sentTime := time.Now()
	resp, err := client.Do(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	PromLatencySum(SiteLatencyGaugeOpt(), site.Name, time.Now().Sub(sentTime))
	if err != nil {
		return err
	}

	// log.Println("site status code ", resp.StatusCode)
	if resp.StatusCode != site.StatusCode {
		return fmt.Errorf("Response statusCode %d unmatch expected %d", site.StatusCode, resp.StatusCode)
	}

	return nil
}

func monitorAlert(site SiteCfg) {
	err := monitorSite(site)
	if err != nil {
		errMsg := fmt.Sprintf("site %s error %v", site.URL, err)
		Alert(errMsg)
	}
}

func mon(site SiteCfg) {
	err := monitorSite(site)
	if err != nil {
		errMsg := fmt.Sprintf("site %s error %v", site.URL, err)
		Alert(errMsg)
	}
}

// MonitorSites monitors a list of sites
func MonitorSites() {
	sites := GetConfig().SitesConfig.Sites
	log.Println(sites)

	for _, site := range sites {
		log.Println(site.URL)
		go func(s SiteCfg) {
			interval := TimeDuration(s.IntervalSeconds, 120, time.Second)
			mon(s)
			for {
				select {
				case <-time.Tick(interval):
					mon(s)
				}
			}
		}(site)
	}
}