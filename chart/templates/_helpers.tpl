{{/* vim: set filetype=mustache: */}}

{{/*
Renders a value that contains template.
Usage:
{{ include "render" ( dict "value" .Values.path.to.the.Value "context" $) }}
*/}}
{{- define "render" -}}
    {{- if typeIs "string" .value }}
        {{- tpl .value .context }}
    {{- else }}
        {{- tpl (.value | toYaml) .context }}
    {{- end }}
{{- end -}}

{{/*
Renders the base init containers for all deployments, if any
Usage:
{{ include "base_init_containers" . }}
*/}}
{{- define "base_init_containers" -}}
    {{- if .Values.base.initContainers.enabled }}
    {{- include "render" (dict "value" .Values.base.initContainers.containers "context" $) | nindent 8 }}
    {{- end }}
    {{- include "jaeger_agent_init_container" . }}
{{- end -}}

{{/*
Renders the CORE server init container, if enabled
Usage:
{{ include "base_init_core_containers" . }}
*/}}
{{- define "base_init_core_containers" -}}
    {{- if .Values.base.initCoreContainers.enabled }}
    {{- include "render" (dict "value" .Values.base.initCoreContainers.containers "context" $) | nindent 8 }}
    {{- end }}
{{- end -}}

{{/*
Renders the jaeger agent init container, if enabled
Usage:
{{ include "jaeger_agent_init_container" . }}
*/}}
{{- define "jaeger_agent_init_container" -}}
    {{- if .Values.base.jaeger.enabled }}
      {{- if .Values.base.jaeger.initContainer }}
      {{- include "render" (dict "value" .Values.base.jaeger.agent.initContainer "context" $) | nindent 8 }}
      {{- end }}
    {{- end }}
{{- end -}}

{{/*
Renders the base image pull secrets for all deployments, if any
Usage:
{{ include "base_pull_secrets" . }}
*/}}
{{- define "base_pull_secrets" -}}
    {{- if .Values.base.imagePullSecrets.enabled }}
    {{- include "render" (dict "value" .Values.base.imagePullSecrets.secrets "context" $) | nindent 8 }}
    {{- end }}
{{- end -}}

{{/*
Renders the REST server init container, if enabled
Usage:
{{- include "rest_agent_init_container" . }}
*/}}
{{- define "rest_agent_init_container" -}}
    {{- if .Values.base.initRestContainer.enabled }}
        {{- include "render" (dict "value" .Values.base.initRestContainer.initContainer "context" $) | nindent 8 }}
    {{- end }}
{{- end -}}

{{/*
Renders the jaeger scheduling rules, if any
Usage:
{{ include "jaeger_scheduling" . }}
*/}}
{{- define "jaeger_scheduling" -}}
    {{- if index .Values "jaeger-operator" "affinity" }}
  affinity:
    {{- include "render" (dict "value" (index .Values "jaeger-operator" "affinity") "context" $) | nindent 4 }}
    {{- end }}
    {{- if index .Values "jaeger-operator" "tolerations" }}
  tolerations:
    {{- include "render" (dict "value" (index .Values "jaeger-operator" "tolerations") "context" $) | nindent 4 }}
    {{- end }}
{{- end -}}

{{/*
Renders the extra volumes for the msp-deployment
Usage:
{{ include "msp_extra_volumes" . | nindent $INDENT }}
*/}}
{{- define "msp_extra_volumes" -}}
    {{- if len .Values.msp.extraVolumes }}
    {{- include "render" (dict "value" .Values.msp.extraVolumes "context" $) }}
    {{- end }}
{{- end -}}

{{/*
Renders the extra volume mounts for the msp-operator container of the msp-deployment
Usage:
{{ include "msp_extra_volume_mounts" . | nindent $INDENT }}
*/}}
{{- define "msp_extra_volume_mounts" -}}
    {{- if len .Values.msp.extraVolumeMounts }}
    {{- include "render" (dict "value" .Values.msp.extraVolumeMounts "context" $) }}
    {{- end }}
{{- end -}}

{{/*
Renders the image for the msp-operator container
Usage:
{{ include "msp_operator_image" . }}
*/}}
{{- define "msp_operator_image" -}}
    {{- if not (empty .Values.msp.image) -}}
        {{- .Values.msp.image -}}
    {{- else }}
        {{- .Values.mayastorCP.registry }}mayadata/mayastor-msp-operator:{{ .Values.mayastorCP.tag }}
    {{- end }}
{{- end -}}

{{/*
Renders the image for the rest container
Usage:
{{ include "rest_image" . }}
*/}}
{{- define "rest_image" -}}
    {{- if not (empty .Values.rest.image) -}}
        {{- .Values.rest.image -}}
    {{- else }}
        {{- .Values.mayastorCP.registry }}mayadata/mayastor-rest:{{ .Values.mayastorCP.tag }}
    {{- end }}
{{- end -}}

{{/*
Renders the image for the csi-controller container
Usage:
{{ include "csi_controller_image" . }}
*/}}
{{- define "csi_controller_image" -}}
    {{- if not (empty .Values.csi.controller.image) -}}
        {{- .Values.csi.controller.image -}}
    {{- else }}
        {{- .Values.mayastorCP.registry }}mayadata/mayastor-csi-controller:{{ .Values.mayastorCP.tag }}
    {{- end }}
{{- end -}}

{{/*
Renders the image for the core container
Usage:
{{ include "core_image" . }}
*/}}
{{- define "core_image" -}}
    {{- if not (empty .Values.core.image) -}}
        {{- .Values.core.image -}}
    {{- else }}
        {{- .Values.mayastorCP.registry }}mayadata/mayastor-core:{{ .Values.mayastorCP.tag }}
    {{- end }}
{{- end -}}
