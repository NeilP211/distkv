{{/*
Expand the name of the chart.
*/}}
{{- define "distkv.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "distkv.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "distkv.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels applied to every resource.
*/}}
{{- define "distkv.labels" -}}
helm.sh/chart: {{ include "distkv.chart" . }}
{{ include "distkv.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels used by Service and StatefulSet selectors.
*/}}
{{- define "distkv.selectorLabels" -}}
app.kubernetes.io/name: {{ include "distkv.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Build the --peers flag value: a comma-separated id=host:port list for all
cluster members.  Each pod's stable DNS name is derived from the StatefulSet
pod index and the headless service name.
*/}}
{{- define "distkv.peers" -}}
{{- $fullname := include "distkv.fullname" . -}}
{{- $ns := .Release.Namespace -}}
{{- $port := .Values.service.port -}}
{{- $svc := $fullname -}}
{{- $peers := list -}}
{{- range $i, $_ := until (int .Values.replicaCount) -}}
  {{- $id := printf "n%d" (add $i 1) -}}
  {{- $host := printf "%s-%d.%s.%s.svc.cluster.local" $fullname $i $svc $ns -}}
  {{- $peers = append $peers (printf "%s=%s:%d" $id $host (int $port)) -}}
{{- end -}}
{{- join "," $peers -}}
{{- end }}
