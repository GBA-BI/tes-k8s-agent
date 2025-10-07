## Bio-OS TES K8s Agent

Bio-OS TES K8s Agent is a component of the [Bio-OS task execution service](https://github.com/GBA-BI/tes-api) , responsible for eceives scheduled tasks from the API server, launches them as Kubernetes Jobs/Pods, monitors execution, and reports task status and logs back.

#### Deployment
For deployment, you can refer to the manifests directory to build and install these services in your own Kubernetes cluster using Helm.


## License
This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.
