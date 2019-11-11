antidote_stats - A collector for antidote statistics
=====

Statistics can be provided to the collector via `gen_server:cast(antidote_stats_collector, STAT)`, 
replacing `STAT` with one of the supported stat types.

Prometheus and Grafana Deployment
=====

The monitoring setup can be found in the [monitoring](monitoring) folder.
Before use, the following files have to be adjusted.

* The [docker-compose file](monitoring/docker-compose.yml): 
    * If Grafana and Prometheus are not supposed to run on the host network, comment `network_mode` out
    * If Prometheus should survive crashses and retain its data, specify a volume for the Prometheus data

* The [Grafana configuration file](monitoring/grafana-config/provisioning/datasources/all.yml):
    * Specifiy the default address of the Prometheus data source

* The [Prometheus configuration file](monitoring/prometheus-config/prometheus.yml):
    * Speficy which nodes to scrape

If the monitoring setup is used inside a docker network, make sure the antidote nodes are reachable from that network by Prometheus.
