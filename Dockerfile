# Mimics the GitHub Actions ubuntu-latest (ubuntu-24.04) runner environment.
# Used to test mirror_photos.py locally before pushing.
#
# Build:
#   docker build -t mco-mirror .
#
# Run (dry-run, using your local AWS profile):
#   docker run --rm \
#     -v ~/.aws:/root/.aws:ro \
#     -v $(pwd)/cache:/repo/cache \
#     mco-mirror --dry-run
#
# Run (full sync):
#   docker run --rm \
#     -v ~/.aws:/root/.aws:ro \
#     -v $(pwd)/cache:/repo/cache \
#     mco-mirror --profile mco
#
# The cache volume mount lets raw/webp files persist across runs so you
# don't re-download everything each time.

FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# Match the apt packages installed by the workflow
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3.12 \
        python3.12-venv \
        python3-pip \
        webp \
    && rm -rf /var/lib/apt/lists/*

# Create a venv (avoids the externally-managed-environment restriction)
ENV VIRTUAL_ENV=/opt/venv
RUN python3.12 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

WORKDIR /repo

COPY scripts/requirements.txt scripts/requirements.txt
RUN pip install --upgrade pip && pip install -r scripts/requirements.txt

COPY scripts/ scripts/

ENTRYPOINT ["python", "scripts/mirror_photos.py"]
