# MOSAIC jobs: the runtime for platform code that has to run inside the stack --
# the embedding-index pipeline (M4) and dataset registration (the N1 hand-off).
#
# A container, not the host interpreter, for the same reason as docker/dbt: the host
# runs whatever Python the operator has (3.14 here, ahead of torch's wheels), and the
# model plus torch are ~1 GB that should resolve identically on every machine.
#
# The model is baked in at a pinned revision and the hub is switched off at run time
# (ADR 0006, M4-C4): a refresh can never pick up a re-uploaded model, so E3/E4 runs are
# comparable, and a demo needs no network. To add a model, add a line below; the
# pipeline looks it up as /opt/models/<org>__<name>.
FROM python:3.11-slim

# CPU-only torch from PyTorch's own index: the default PyPI wheel drags in ~3 GB of
# CUDA libraries this machine cannot use.
RUN pip install --no-cache-dir torch==2.4.1 --index-url https://download.pytorch.org/whl/cpu \
 && pip install --no-cache-dir \
        "sentence-transformers==3.0.1" "transformers==4.44.2" "numpy==1.26.4" \
        "psycopg[binary]==3.2.3" "PyYAML==6.0.2" "huggingface_hub==0.24.7"

ARG MODEL=sentence-transformers/all-MiniLM-L6-v2
ARG MODEL_REVISION=1110a243fdf4706b3f48f1d95db1a4f5529b4d41
COPY fetch_model.py /tmp/fetch_model.py
RUN python /tmp/fetch_model.py "${MODEL}" "${MODEL_REVISION}" && rm /tmp/fetch_model.py

ENV HF_HUB_OFFLINE=1 \
    TRANSFORMERS_OFFLINE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/opt/mosaic/lib

# platform/ is mounted as mosaic_platform, never as `platform`: that name would shadow
# the standard library's platform module, which torch and transformers import.
WORKDIR /opt/mosaic
ENTRYPOINT ["python", "-m"]
CMD ["embeddings", "--help"]
