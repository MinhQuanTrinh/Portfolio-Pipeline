# Streamlit image — deps installed at build time (avoids pip-on-every-container-start).
#
# If you see SSLCertVerificationError to files.pythonhosted.org, your network likely uses
# TLS inspection (corporate proxy). Fix ONE of:
#   1) Drop your proxy ROOT CA as dashboard/certs/*.pem or *.crt (PEM). It is copied in below.
#   2) On the machine that builds this image, fix Docker Desktop / daemon trust store.
#
# Trusted-host + long timeouts help some environments; they are not a substitute for a proper CA.
FROM python:3.11-slim
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
# Optional: corporate / proxy root CAs (add your own files under dashboard/certs/)
COPY dashboard/certs/ /tmp/extra-certs/
RUN set -eux; \
    for f in /tmp/extra-certs/*; do \
      [ -f "$f" ] || continue; \
      case "$f" in \
        *.pem|*.crt) cp "$f" /usr/local/share/ca-certificates/ ;; \
      esac; \
    done; \
RUN find /tmp/extra-certs -maxdepth 1 -type f \( -name '*.pem' -o -name '*.crt' \) \
        -exec cp -t /usr/local/share/ca-certificates/ {} + 2>/dev/null || true; \
    update-ca-certificates
COPY dashboard/ /app/
ENV PIP_REQUIRE_HASHES=0
ENV PIP_DEFAULT_TIMEOUT=300
ENV PIP_RETRIES=15
# Space-separated list; tells pip not to verify TLS for these hosts (common workaround behind broken MITM setups).
ENV PIP_TRUSTED_HOST="pypi.org pypi.python.org files.pythonhosted.org"
RUN pip install --no-cache-dir \
    --trusted-host pypi.org \
    --trusted-host pypi.python.org \
    --trusted-host files.pythonhosted.org \
    streamlit==1.35.0 \
    plotly==5.22.0 \
    duckdb==0.10.3 \
    pandas==2.2.2 \
    pyarrow==16.0.0 \
    boto3==1.34.0
EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.address", "0.0.0.0", "--server.port", "8501"]
