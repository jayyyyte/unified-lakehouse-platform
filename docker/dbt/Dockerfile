# MOSAIC dbt: dbt-core + dbt-trino, for Silver and Gold (ADR 0003).
#
# A container rather than the host interpreter: the host runs whatever Python the
# operator has (3.14 on the reference machine), which dbt-core does not support, and
# a pinned image makes every build resolve the same dbt.
#
# Entrypoint = assemble, then dbt. assemble.py generates one Silver model per captured
# table from the domains' connector configs and copies each domain's Gold models into
# the project, so the dbt project itself never names a domain (L3).
FROM python:3.11-slim

RUN pip install --no-cache-dir "dbt-core==1.8.8" "dbt-trino==1.8.3"

WORKDIR /opt/mosaic/transform
ENV DBT_PROFILES_DIR=/opt/mosaic/transform \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false \
    PYTHONUNBUFFERED=1

ENTRYPOINT ["sh", "-c", "python assemble.py && exec dbt \"$@\"", "--"]
CMD ["build"]
