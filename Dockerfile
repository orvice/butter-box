FROM golang:1.26 AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -o /out/butter-box .

FROM ubuntu:24.04

ARG GO_VERSION=1.26.5
ARG NODE_MAJOR=22

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

# Python
RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
		python3 \
		python3-pip \
		python3-venv \
	&& rm -rf /var/lib/apt/lists/* \
	&& ln -sf /usr/bin/python3 /usr/local/bin/python

# Node.js (NodeSource)
RUN curl -fsSL "https://deb.nodesource.com/setup_${NODE_MAJOR}.x" | bash - \
	&& apt-get install -y --no-install-recommends nodejs \
	&& rm -rf /var/lib/apt/lists/*

# Google Workspace CLI (gws), installed system-wide
RUN npm install -g @googleworkspace/cli \
	&& npm cache clean --force

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
