package app

import (
	"fmt"

	"github.com/GBA-BI/tes-k8s-agent/pkg/log"
	"github.com/go-logr/zapr"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"

	"github.com/GBA-BI/tes-k8s-agent/pkg/accelerate"
	"github.com/GBA-BI/tes-k8s-agent/pkg/app/options"
	"github.com/GBA-BI/tes-k8s-agent/pkg/cluster"
	"github.com/GBA-BI/tes-k8s-agent/pkg/crontab"
	"github.com/GBA-BI/tes-k8s-agent/pkg/localstore"
	"github.com/GBA-BI/tes-k8s-agent/pkg/offload"
	"github.com/GBA-BI/tes-k8s-agent/pkg/reconciler"
	"github.com/GBA-BI/tes-k8s-agent/pkg/reconciler/runner"
	"github.com/GBA-BI/tes-k8s-agent/pkg/syncer"
	"github.com/GBA-BI/tes-k8s-agent/pkg/version"
	"github.com/GBA-BI/tes-k8s-agent/pkg/vetesclient"
	"github.com/GBA-BI/tes-k8s-agent/pkg/viper"
)

var component = "vetes-k8s-agent"

func newAgentCommand(opts *options.Options) *cobra.Command {
	return &cobra.Command{
		Use:          component,
		Short:        "veTES k8s agent",
		Long:         "veTES k8s agent",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			version.PrintVersionOrContinue()

			if err := opts.Validate(); err != nil {
				return err
			}

			log.RegisterLogger(opts.Log)
			defer log.Sync()

			cmd.Flags().VisitAll(func(flag *pflag.Flag) {
				log.Infow("FLAG", flag.Name, flag.Value)
			})

			return run(opts)
		},
	}
}

func run(opts *options.Options) error {
	log.Infow("run veTES k8s agent")

	kubeConfig, err := ctrl.GetConfig()
	if err != nil {
		return fmt.Errorf("failed to load kubeconfig: %w", err)
	}

	if zapLogger := log.GetZapLogger(); zapLogger != nil {
		ctrl.SetLogger(zapr.NewLogger(zapLogger))
	}
	mgr, err := ctrl.NewManager(kubeConfig, ctrl.Options{
		Cache: cache.Options{
			Namespaces: []string{opts.Namespace},
		},
		LeaderElection:          opts.LeaderElection.Enable,
		LeaderElectionNamespace: opts.LeaderElection.Namespace,
		LeaderElectionID:        opts.LeaderElection.Name,
		MetricsBindAddress:      fmt.Sprintf(":%d", opts.Server.MetricsPort),
		HealthProbeBindAddress:  fmt.Sprintf(":%d", opts.Server.HealthzPort),
	})
	if err != nil {
		return fmt.Errorf("failed to create manager: %w", err)
	}
	if err = mgr.AddHealthzCheck("ping", healthz.Ping); err != nil {
		return fmt.Errorf("unable to create healthz check: %w", err)
	}

	vetesClient := vetesclient.NewClient(opts.VeTESClient)
	offloadHelper, err := offload.NewHelper(opts.Offload)
	if err != nil {
		return err
	}
	kubeClientNative := kubernetes.NewForConfigOrDie(kubeConfig)
	kubeClient := mgr.GetClient()
	localStoreHelper := localstore.NewHelper(kubeClient, opts.Namespace)
	accelerator, err := accelerate.NewAccelerator(vetesClient, kubeClient, opts.Namespace, opts.Accelerate)
	if err != nil {
		return err
	}
	runnerImpl, err := runner.New(vetesClient, localStoreHelper, offloadHelper, accelerator, kubeClientNative, kubeClient, opts.Cluster.ID, opts.Namespace, opts.Runner)
	if err != nil {
		return err
	}

	if err = setupCrontab(mgr, vetesClient, localStoreHelper, offloadHelper, accelerator, runnerImpl, opts); err != nil {
		return fmt.Errorf("failed to setup crontab: %w", err)
	}

	if err = setupReconcilers(mgr, localStoreHelper, runnerImpl, opts); err != nil {
		return fmt.Errorf("failed to set up reconcilers: %w", err)
	}

	log.Infow("starting manager")
	ctx := ctrl.SetupSignalHandler()
	if err = mgr.Start(ctx); err != nil {
		return fmt.Errorf("problem in running manager: %w", err)
	}
	return nil
}

// NewAgentCommand create a veTES k8s agent command.
func NewAgentCommand() (*cobra.Command, error) {
	opts := options.NewOptions()
	cmd := newAgentCommand(opts)

	opts.AddFlags(cmd.Flags())
	version.AddFlags(cmd.Flags())
	cmd.Flags().AddFlag(pflag.Lookup(viper.ConfigFlagName))
	if err := viper.LoadConfig(opts); err != nil {
		return nil, err
	}
	return cmd, nil
}

func setupCrontab(mgr ctrl.Manager, vetesClient vetesclient.Client, localStoreHelper localstore.Helper,
	offloadHelper offload.Helper, accelerator accelerate.Accelerator, runnerImpl *runner.Runner, opts *options.Options) error {
	cron := crontab.NewCrontab()
	if err := cluster.RegisterCronjob(cron, vetesClient, opts.Cluster); err != nil {
		return err
	}
	if err := accelerate.RegisterCrontab(cron, accelerator); err != nil {
		return err
	}
	if err := runner.RegisterCrontab(cron, runnerImpl); err != nil {
		return err
	}
	if err := syncer.RegisterCrontab(cron, vetesClient, localStoreHelper, offloadHelper, accelerator, opts.Cluster.ID, opts.Syncer); err != nil {
		return err
	}
	return mgr.Add(cron)
}

func setupReconcilers(mgr ctrl.Manager, localStoreHelper localstore.Helper, runnerImpl *runner.Runner, opts *options.Options) error {
	if err := reconciler.RegisterReconciler(mgr, localStoreHelper, runnerImpl, opts.Reconciler); err != nil {
		return err
	}
	return nil
}
