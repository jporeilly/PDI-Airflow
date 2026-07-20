#!/bin/bash
# Master-only Carte entrypoint — simplified from the course's
# docker-entrypoint.sh (no slave/master federation in the lab).
set -e

if [ "$1" = 'carte.sh' ]; then
  if [ ! -f "$KETTLE_HOME/carte.config.xml" ]; then
    : ${CARTE_NAME:=lab-carte}
    : ${CARTE_PORT:=8181}
    : ${CARTE_USER:=cluster}
    : ${CARTE_PASSWORD:=cluster}

    cp "$PENTAHO_HOME/templates/carte-config.xml" "$KETTLE_HOME/carte.config.xml"
    sed -i "s/CARTE_NAME/$CARTE_NAME/" "$KETTLE_HOME/carte.config.xml"
    sed -i "s/CARTE_PORT/$CARTE_PORT/" "$KETTLE_HOME/carte.config.xml"
    sed -i "s/CARTE_USER/$CARTE_USER/" "$KETTLE_HOME/carte.config.xml"
    sed -i "s/CARTE_PASSWORD/$CARTE_PASSWORD/" "$KETTLE_HOME/carte.config.xml"
  fi
fi

exec "$@"
