package cluster

import (
	"context"
	"fmt"
	"os"

	"github.com/GBA-BI/tes-k8s-agent/pkg/log"
	"gopkg.in/yaml.v3"

	"github.com/GBA-BI/tes-k8s-agent/pkg/crontab"
	"github.com/GBA-BI/tes-k8s-agent/pkg/vetesclient"
)

type reporter struct {
	vetesClient vetesclient.Client
	id          string
	cfg         *Config
}

// RegisterCronjob ...
func RegisterCronjob(cron *crontab.Crontab, vetesClient vetesclient.Client, opts *Options) error {
	data, err := os.ReadFile(opts.ConfigPath)
	if err != nil {
		return fmt.Errorf("failed to read %s: %w", opts.ConfigPath, err)
	}
	cfg := new(Config)
	if err = yaml.Unmarshal(data, cfg); err != nil {
		return fmt.Errorf("failed to unmarshal cluster config: %w", err)
	}

	r := &reporter{
		vetesClient: vetesClient,
		id:          opts.ID,
		cfg:         cfg,
	}
	return cron.RegisterCron(opts.ReportPeriod, r.reportCluster)
}

func (r *reporter) reportCluster() {
	ctx := context.Background()
	req := convertToClientCluster(r.id, r.cfg)
	if _, err := r.vetesClient.PutCluster(ctx, req); err != nil {
		log.Errorw("put cluster failed", "err", err)
	}
}
