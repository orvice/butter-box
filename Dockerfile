FROM golang:1.27 AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -o /out/butter-box .

FROM ubuntu:24.04

ARG GO_VERSION=1.27.1
ARG NODE_MAJOR=24
ARG LOKI_VERSION=3.7.7
ARG CURSOR_SDK_BRIDGE_VERSION=1.0.31
ARG CURSOR_SDK_BRIDGE_SHA256_AMD64=527cbebdc6aad4ea7d3026f49b4879e3e7f3d6e907c0598241863802b022c838
ARG CURSOR_SDK_BRIDGE_SHA256_ARM64=c5b3dce52ba01f60b152861f008e8d2c931c0ba9fbf79a110ddea375ae40717b

# Base CLI tools
RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
		bash \
		build-essential \
		ca-certificates \
		curl \
		git \
		gnupg \
		jq \
		less \
		openssh-client \
		ripgrep \
		sudo \
		unzip \
		vim \
		wget \
		xz-utils \
		zip \
	&& rm -rf /var/lib/apt/lists/*

# Cursor SDK Bridge (pinned release; the archive contains the self-contained
# binary and its sdk.v1 manifest).
RUN set -eux; \
  case "$(dpkg --print-architecture)" in \
    amd64) BRIDGE_ARCH=x64; BRIDGE_SHA256="${CURSOR_SDK_BRIDGE_SHA256_AMD64}" ;; \
    arm64) BRIDGE_ARCH=arm64; BRIDGE_SHA256="${CURSOR_SDK_BRIDGE_SHA256_ARM64}" ;; \
    *) echo "unsupported architecture for cursor-sdk-bridge: $(dpkg --print-architecture)" >&2; exit 1 ;; \
  esac; \
  ASSET="cursor-sdk-bridge-standalone-linux-${BRIDGE_ARCH}.tar.gz"; \
  URL="https://github.com/cursor/sdk-bridge/releases/download/v${CURSOR_SDK_BRIDGE_VERSION}/${ASSET}"; \
  curl -fsSL "${URL}" -o "/tmp/${ASSET}"; \
  echo "${BRIDGE_SHA256}  /tmp/${ASSET}" | sha256sum -c -; \
  mkdir -p /tmp/cursor-sdk-bridge; \
  tar -xzf "/tmp/${ASSET}" -C /tmp/cursor-sdk-bridge; \
  test "$(jq -r .protocol /tmp/cursor-sdk-bridge/manifest.json)" = "sdk.v1"; \
  test "$(jq -r .sdkVersion /tmp/cursor-sdk-bridge/manifest.json)" = "${CURSOR_SDK_BRIDGE_VERSION}"; \
  test -x /tmp/cursor-sdk-bridge/bin/cursor-sdk-bridge; \
  install -m 0755 /tmp/cursor-sdk-bridge/bin/cursor-sdk-bridge /usr/local/bin/cursor-sdk-bridge; \
  rm -rf "/tmp/${ASSET}" /tmp/cursor-sdk-bridge

# Python
RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
		python3 \
		python3-pip \
		python3-venv \
	&& rm -rf /var/lib/apt/lists/* \
	&& ln -sf /usr/bin/python3 /usr/local/bin/python

# toot (Mastodon CLI, https://toot.bezdomni.net)
RUN pip3 install --no-cache-dir --break-system-packages "toot[images]"

# Node.js (NodeSource)
RUN curl -fsSL "https://deb.nodesource.com/setup_${NODE_MAJOR}.x" | bash - \
	&& apt-get install -y --no-install-recommends nodejs \
	&& rm -rf /var/lib/apt/lists/*

# Node.js CLIs, installed system-wide
RUN npm install -g \
		@agegr/pi-web \
		@doist/todoist-cli \
		@earendil-works/pi-coding-agent \
		@googleworkspace/cli \
		@openai/codex \
		corepack \
		npm \
		opencode-ai \
	&& npm cache clean --force

# GitHub CLI (gh)
RUN wget -nv -O /tmp/githubcli-keyring.gpg https://cli.github.com/packages/githubcli-archive-keyring.gpg \
	&& install -o root -g root -m 644 /tmp/githubcli-keyring.gpg /etc/apt/keyrings/githubcli-archive-keyring.gpg \
	&& echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" > /etc/apt/sources.list.d/github-cli.list \
	&& apt-get update \
	&& apt-get install -y --no-install-recommends gh \
	&& rm -rf /var/lib/apt/lists/* /tmp/githubcli-keyring.gpg

# Go toolchain
RUN ARCH="$(dpkg --print-architecture)" \
	&& curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-${ARCH}.tar.gz" -o /tmp/go.tgz \
	&& tar -C /usr/local -xzf /tmp/go.tgz \
	&& rm /tmp/go.tgz

# kubectl (latest stable at build time)
RUN ARCH="$(dpkg --print-architecture)" \
	&& KUBECTL_VERSION="$(curl -fsSL https://dl.k8s.io/release/stable.txt)" \
	&& curl -fsSL "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${ARCH}/kubectl" -o /usr/local/bin/kubectl \
	&& chmod +x /usr/local/bin/kubectl

# AWS CLI v2
RUN ARCH="$(uname -m)" \
	&& curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-${ARCH}.zip" -o /tmp/awscliv2.zip \
	&& unzip -q /tmp/awscliv2.zip -d /tmp \
	&& /tmp/aws/install \
	&& rm -rf /tmp/aws /tmp/awscliv2.zip

# Google Cloud CLI
RUN curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
	&& echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" > /etc/apt/sources.list.d/google-cloud-sdk.list \
	&& apt-get update \
	&& apt-get install -y --no-install-recommends google-cloud-cli \
	&& rm -rf /var/lib/apt/lists/*

# rclone
RUN curl -fsSL https://rclone.org/install.sh | bash

# logcli (Grafana Loki CLI)
RUN ARCH="$(dpkg --print-architecture)" \
	&& curl -fsSL "https://github.com/grafana/loki/releases/download/v${LOKI_VERSION}/logcli_${LOKI_VERSION}_${ARCH}.deb" -o /tmp/logcli.deb \
	&& dpkg -i /tmp/logcli.deb \
	&& rm /tmp/logcli.deb

# gog cli
RUN GOPATH=/tmp/gopath GOBIN=/usr/local/bin /usr/local/go/bin/go install github.com/openclaw/gogcli/cmd/gog@latest \
	&& rm -rf /tmp/gopath

# GitLab CLI (glab)
RUN GOPATH=/tmp/gopath GOBIN=/usr/local/bin /usr/local/go/bin/go install gitlab.com/gitlab-org/cli/cmd/glab@latest \
	&& rm -rf /tmp/gopath

RUN useradd --uid 10001 --create-home --shell /bin/bash butterbox \
	&& mkdir -p /workspace \
	&& chown -R butterbox:butterbox /workspace \
	&& echo "butterbox ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/butterbox \
	&& chmod 0440 /etc/sudoers.d/butterbox

WORKDIR /workspace

COPY --from=builder /out/butter-box /usr/local/bin/butter-box

ENV MCP_ADDR=:8080
ENV MCP_HTTP_PATH=/mcp
ENV SANDBOX_ROOT=/workspace
ENV SANDBOX_SHELL=/bin/bash

ENV HOME=/home/butterbox
ENV GOPATH=/home/butterbox/go
ENV NPM_CONFIG_PREFIX=/home/butterbox/.npm-global
ENV PIP_BREAK_SYSTEM_PACKAGES=1
ENV PATH=/usr/local/go/bin:/home/butterbox/go/bin:/home/butterbox/.npm-global/bin:/home/butterbox/.local/bin:$PATH

EXPOSE 8080

USER butterbox

ENTRYPOINT ["/usr/local/bin/butter-box"]
