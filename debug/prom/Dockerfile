FROM prom/prometheus
ADD prometheus.yml /etc/prometheus/

USER ROOT
ADD tgroups/ ./tgroups 
ADD change_config .
