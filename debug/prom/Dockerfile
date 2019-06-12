FROM prom/prometheus
ADD prometheus.yml /etc/prometheus/


RUN groupadd -g 1000 admin && \
    useradd -r -u 1000 -g admin admin
USER admin


USER admin
ADD tgroups/ ./tgroups 
ADD change_config .
