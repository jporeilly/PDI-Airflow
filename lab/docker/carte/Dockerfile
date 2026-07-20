# Containerized PDI + Carte for the lab — modernized from the
# "How-To: Apache Airflow" course's setup-pentaho image (2023):
# Java 8 -> 11, PDI 9.1 -> 9.4, master-only entrypoint.
#
# Build is heavy (~1.5 GB PDI download from SourceForge) — it runs only
# with the compose profile:  docker compose --profile carte up -d
FROM eclipse-temurin:11-jre

LABEL description="Pentaho Data Integration + Carte server for the PDI-AirFlow lab"

ENV PDI_RELEASE=9.4 \
    PDI_VERSION=9.4.0.0-343 \
    PENTAHO_HOME=/home/pentaho \
    KETTLE_HOME=/opt/data-integration \
    PATH=/opt/data-integration:${PATH}

RUN apt-get update && apt-get install -y --no-install-recommends wget unzip \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p ${PENTAHO_HOME}/templates ${PENTAHO_HOME}/scripts \
    && useradd -s /bin/bash -d ${PENTAHO_HOME} pentaho \
    && wget --progress=dot:giga \
       "https://sourceforge.net/projects/pentaho/files/Pentaho-${PDI_RELEASE}/client-tools/pdi-ce-${PDI_VERSION}.zip" \
       -P ${PENTAHO_HOME} \
    && unzip -q ${PENTAHO_HOME}/pdi-ce-${PDI_VERSION}.zip -d /opt/ \
    && rm ${PENTAHO_HOME}/pdi-ce-${PDI_VERSION}.zip \
    && mkdir -p ${KETTLE_HOME}/.kettle /home/pentaho/repositories \
    && chown -R pentaho ${PENTAHO_HOME} ${KETTLE_HOME}

COPY carte-config.xml ${PENTAHO_HOME}/templates/
COPY repositories.xml ${KETTLE_HOME}/.kettle/repositories.xml
COPY docker-entrypoint.sh ${PENTAHO_HOME}/scripts/
RUN chmod +x ${PENTAHO_HOME}/scripts/docker-entrypoint.sh \
    && chown pentaho ${PENTAHO_HOME}/templates/carte-config.xml \
                     ${KETTLE_HOME}/.kettle/repositories.xml

USER pentaho
EXPOSE 8181
WORKDIR ${KETTLE_HOME}

ENTRYPOINT ["/home/pentaho/scripts/docker-entrypoint.sh"]
CMD ["carte.sh", "carte.config.xml"]
