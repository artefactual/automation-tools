FROM compose_archivematica-dashboard

USER root

# Check the build time arguments.
ARG transfer_source_uuid
RUN [ ! -z "${transfer_source_uuid}" ]

# Install the system dependencies and create all the automation-tools locations.
RUN apt-get update && \
    apt-get install -y --no-install-recommends p7zip-full

RUN mkdir -p /usr/lib/archivematica/automation-tools && \
    mkdir -p /usr/share/python/automation-tools && \
    mkdir -p /var/log/archivematica/automation-tools && \
    mkdir -p /var/archivematica/automation-tools && \
    mkdir -p /etc/archivematica/automation-tools

RUN chown -R archivematica:archivematica \
    /usr/lib/archivematica/automation-tools \
    /usr/share/python/automation-tools \
    /var/log/archivematica/automation-tools \
    /var/archivematica/automation-tools \
    /etc/archivematica/automation-tools

# Copy the automation-tools and install the local dependencies.
USER archivematica

ADD --chown=archivematica:archivematica \
    ./ /usr/lib/archivematica/automation-tools/
ADD --chown=archivematica:archivematica \
    ./etc/transfers.conf /etc/archivematica/automation-tools/

RUN pip install --user virtualenv

ENV VIRTUAL_ENV=/usr/share/python/automation-tools/venv
RUN python -m virtualenv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN pip install -r /usr/lib/archivematica/automation-tools/requirements.txt

# Configure the automation-tools installation.
ENV TRANSFER_SOURCE=$transfer_source_uuid

ADD --chown=archivematica:archivematica \
    ./docker-setup/transfer-script.sh /etc/archivematica/automation-tools/

RUN chmod u+x /etc/archivematica/automation-tools/transfer-script.sh

ADD --chown=archivematica:archivematica \
    ./docker-setup/get-accession-number \
    /usr/lib/archivematica/automation-tools/transfers/get-accession-number

ADD --chown=archivematica:archivematica \
    ./docker-setup/pre-transfer \
    /usr/lib/archivematica/automation-tools/transfers/pre-transfer