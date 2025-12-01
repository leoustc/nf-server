FROM ubuntu:22.04 AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates wget bzip2 ssh awscli tree vim && \
    rm -rf /var/lib/apt/lists/*

# Miniforge for Python, Terraform, and OCI CLI
RUN wget -qO /tmp/miniforge.sh https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh && \
    bash /tmp/miniforge.sh -b -p /opt/conda && \
    rm /tmp/miniforge.sh && \
    /opt/conda/bin/conda config --system --set channel_priority strict && \
    /opt/conda/bin/conda install -y python=3.11 conda-forge::terraform conda-forge::oci-cli && \
    /opt/conda/bin/conda clean -afy

ENV PATH=/opt/conda/bin:$PATH

RUN pip install --no-cache-dir pymysql cryptography ansible pydantic==1.10.13

WORKDIR /app

FROM base AS runtime

WORKDIR /app

COPY . .

RUN chmod +x /app/entry.sh /app/oci/autorun.sh

ENTRYPOINT ["/app/entry.sh"]
CMD ["python", "-u", "proxy.py"]
