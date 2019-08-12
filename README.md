antidote_stats - A collector for antidote statistics
=====

Current statistics are provided:

* `open_transaction`
* `transaction_aborted`
* `transaction_finished`
* `operation_read_async`
* `operation_read`
* `operation_update`
* `log_error`
* `{update_staleness, VAL}`



They can be used with `gen_server:cast(antidote_stats_collector, STAT)`, 
replacing `STAT` with one of the supported stat types.
