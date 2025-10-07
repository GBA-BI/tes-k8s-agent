package crontab

import (
	"context"
	"fmt"
	"time"

	"github.com/GBA-BI/tes-k8s-agent/pkg/log"
	"github.com/robfig/cron/v3"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// Crontab ...
type Crontab struct {
	crontab *cron.Cron
}

// NewCrontab ...
func NewCrontab() *Crontab {
	return &Crontab{crontab: cron.New()}
}

var _ manager.Runnable = (*Crontab)(nil)
var _ manager.LeaderElectionRunnable = (*Crontab)(nil)

// NeedLeaderElection ...
func (c *Crontab) NeedLeaderElection() bool {
	return true
}

// Start ...
func (c *Crontab) Start(ctx context.Context) error {
	log.Infow("crontab start")
	c.crontab.Start()
	<-ctx.Done()
	stopCtx := c.crontab.Stop()
	log.Infow("Shutdown signal received, waiting for all cronjob to finish")
	<-stopCtx.Done() // wait all cronjob finished
	log.Infow("All cronjob finished")
	return nil
}

// RegisterCron ...
func (c *Crontab) RegisterCron(period time.Duration, fn func()) error {
	runningFlag := false
	if _, err := c.crontab.AddFunc(fmt.Sprintf("@every %s", period.String()), func() {
		if runningFlag {
			return
		}
		runningFlag = true
		fn()
		runningFlag = false
	}); err != nil {
		return err
	}
	return nil
}
