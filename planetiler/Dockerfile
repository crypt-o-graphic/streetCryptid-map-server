# syntax=docker/dockerfile:1
# The planet bake: Planetiler's OpenMapTiles profile with one change, OSM land
# cover from z5 instead of z7 (landcover-low-zoom.patch; see README "Land cover
# below z7"). Built and published by CI as ghcr.io/<owner>/streetcryptid-planetiler,
# a drop-in for ghcr.io/onthegomap/planetiler in the bake CronJob: same
# arguments, same output.

# --- import helper -----------------------------------------------------------
FROM golang:1.26-bookworm AS helper
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY cmd/server ./cmd/server
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -o /import-helper ./cmd/server

# --- build stage -------------------------------------------------------------
FROM eclipse-temurin:25-jdk-noble AS build

RUN apt-get update \
 && apt-get install -y --no-install-recommends git ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Pinned to the planetiler-openmaptiles commit that builds against Planetiler
# 0.10.2, the engine the stock image we replace ran. Bump deliberately, and
# re-check that the patch still applies and still means the same thing.
ARG OMT_REPO=https://github.com/openmaptiles/planetiler-openmaptiles.git
ARG OMT_COMMIT=4183c01cb31571d9481d3d316e584df53f989e3f

WORKDIR /src
RUN git init -q . \
 && git remote add origin "$OMT_REPO" \
 && git fetch -q --depth 1 origin "$OMT_COMMIT" \
 && git checkout -q FETCH_HEAD

COPY planetiler/landcover-low-zoom.patch /tmp/landcover-low-zoom.patch
RUN git apply --verbose /tmp/landcover-low-zoom.patch

# The landcover tests the patch rewrites run here, so a patch that applies but
# no longer does what it says cannot be published. The rest of upstream's suite
# is upstream's to keep green.
RUN ./mvnw -B -q -Dtest=LandcoverTest -Dsurefire.failIfNoSpecifiedTests=false package \
 && cp target/planetiler-openmaptiles-*-with-deps.jar /out.jar

# Patch only the pinned engine's progress-size helper. Files.walk may throw
# UncheckedIOException when sort removes a chunk during a metrics sample.
ARG ENGINE_COMMIT=0e5588c4a6e8c29a270a33afe8df62027d889604
COPY planetiler/progress-walk.patch /tmp/progress-walk.patch
RUN git init -q /engine \
 && git -C /engine fetch -q --depth 1 https://github.com/onthegomap/planetiler.git "$ENGINE_COMMIT" \
 && git -C /engine checkout -q FETCH_HEAD -- planetiler-core/src/main/java/com/onthegomap/planetiler/util/FileUtils.java \
 && git -C /engine apply --verbose /tmp/progress-walk.patch \
 && javac -cp /out.jar -d /patched /engine/planetiler-core/src/main/java/com/onthegomap/planetiler/util/FileUtils.java \
 && jar --update --file /out.jar -C /patched com/onthegomap/planetiler/util/FileUtils.class

# --- final stage -------------------------------------------------------------
FROM eclipse-temurin:25-jre-noble

RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-boto3 ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY --from=build /out.jar /app/planetiler.jar
COPY --from=helper /import-helper /usr/local/bin/streetcryptid-map-server
COPY planetiler/bake.py /app/bake.py

ENV PYTHONUNBUFFERED=1

# Runs as an arbitrary non-root uid in the cluster; every path it writes is
# passed on the command line (--tmpdir, --download-dir, --output).
USER 65532:65532
ENTRYPOINT ["python3", "/app/bake.py"]
