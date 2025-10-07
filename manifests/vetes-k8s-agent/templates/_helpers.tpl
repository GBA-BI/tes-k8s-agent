{{/*
Expand the name of the chart.
*/}}
{{- define "vetes-k8s-agent.name" -}}
{{- .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "vetes-k8s-agent.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "vetes-k8s-agent.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "vetes-k8s-agent.labels" -}}
helm.sh/chart: {{ include "vetes-k8s-agent.chart" . }}
{{ include "vetes-k8s-agent.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- if .Values.labels }}
{{ toYaml .Values.labels }}
{{- end }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "vetes-k8s-agent.selectorLabels" -}}
app.kubernetes.io/name: {{ include "vetes-k8s-agent.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Return the proper image name
*/}}
{{- define "vetes-k8s-agent.image" -}}
{{- $repositoryName := .Values.platformConfig.imageRepositoryRelease -}}
{{- $imageName := .Values.image.name -}}
{{- $tag := (default .Chart.AppVersion .Values.image.tag) | toString -}}
{{- if .Values.platformConfig.imageRegistry -}}
{{- $registryName := .Values.platformConfig.imageRegistry -}}
{{- printf "%s/%s/%s:%s" $registryName $repositoryName $imageName $tag -}}
{{- else -}}
{{- printf "%s/%s:%s" $repositoryName $imageName $tag -}}
{{- end -}}
{{- end -}}

{{/*
Return the filer image name
*/}}
{{- define "vetes-k8s-agent.filer.image" -}}
{{- $repositoryName := .Values.platformConfig.imageRepositoryRelease -}}
{{- $imageName := .Values.filerImage.name -}}
{{- $tag := .Values.filerImage.tag | toString -}}
{{- if .Values.platformConfig.imageRegistry -}}
{{- $registryName := .Values.platformConfig.imageRegistry -}}
{{- printf "%s/%s/%s:%s" $registryName $repositoryName $imageName $tag -}}
{{- else -}}
{{- printf "%s/%s:%s" $repositoryName $imageName $tag -}}
{{- end -}}
{{- end -}}

{{/*
Create the log-collector resource name
*/}}
{{- define "vetes-k8s-agent.log-collector.name" -}}
{{ include "vetes-k8s-agent.fullname" . | trunc 49 | trimSuffix "-" }}-log-collector
{{- end -}}

{{/*
log-collector common labels
*/}}
{{- define "vetes-k8s-agent.log-collector.labels" -}}
helm.sh/chart: {{ include "vetes-k8s-agent.chart" . }}
{{ include "vetes-k8s-agent.log-collector.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- if .Values.labels }}
{{ toYaml .Values.labels }}
{{- end }}
{{- end }}

{{/*
log-collector selector labels
*/}}
{{- define "vetes-k8s-agent.log-collector.selectorLabels" -}}
app.kubernetes.io/name: {{ include "vetes-k8s-agent.name" . | trunc 49 | trimSuffix "-" }}-log-collector
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}