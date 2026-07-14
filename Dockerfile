# A runnable image of the SaaS service. Point it at object storage with env
# (see resources/config.edn): SAAS_TIER, S3_BUCKET, S3_ENDPOINT, AWS_* creds.
FROM clojure:temurin-21-tools-deps AS app
WORKDIR /app

# Prefetch deps in a cacheable layer (incl. :kabel + :lmdb for Tier-4 streaming).
COPY deps.edn .
RUN clojure -P -M:run:kabel:lmdb

COPY . .

ENV SAAS_TIER=tier2 PORT=8888
EXPOSE 8888
CMD ["clojure", "-M:run"]
