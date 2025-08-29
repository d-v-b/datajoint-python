ARG PY_VER
ARG DISTRO
FROM mcr.microsoft.com/devcontainers/python:${PY_VER}-${DISTRO}

RUN \
apt update && \
apt-get install bash-completion graphviz default-mysql-client curl -y && \
curl -fsSL https://deb.nodesource.com/setup_lts.x | bash - && \
apt-get install -y nodejs && \
pip install flake8 black faker ipykernel pytest pytest-cov nose nose-cov datajoint jupyterlab && \
pip uninstall datajoint -y && \
apt-get clean && \
rm -rf /var/lib/apt/lists/*

USER root
ENV DJ_HOST db
ENV DJ_USER root
ENV DJ_PASS password