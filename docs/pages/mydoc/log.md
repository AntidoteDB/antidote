---
title: Caches and logs
last_updated: May 19, 2017
tags: [architecture]
sidebar: mydoc_sidebar
permalink: log.html
folder: mydoc
---
# Intro
Logs and caches will be explained using two scripts ```cache_dump.erl``` and ```log_dump.erl```,
both available [here](https://github.com/lplit/antidote-tools/tree/master/tables_dump). 

## Dependencies

### General 

[Antidote](https://github.com/SyncFree/antidote)

[Erlang Remote Procedure Calls](http://erlang.org/doc/man/rpc.html)
All of the interaction with Antidote is executed via RPC calls


### ```cache_dump.erl``` specific

[ETS - Erlang Term Storage](http://erlang.org/doc/man/ets.html)
The script uses RPCs and pulls information from Erlang's built-in storage tables used to store the caches and snapshots


### ```log_dump.erl``` specific 

[Erlang Disk Log](http://erlang.org/doc/man/disk_log.html)
Antidote uses the [Erlang Disk Log](http://erlang.org/doc/man/disk_log.html) module to handle logs internally. 


## Caches

### Available tables discovery
All the calls are executed with RPC inside the script, but will be explained as local calls for simplicity's sake. Example outputs have also been shortened, to keep this page concise.

We use ```ets:all()``` to retrieve a list of all the tables available at the node we're connecting to, example output should yield something like this:
{% raw %}
```erlang
> ets:all().
> [69927303,8978822,
'snapshot_cache-1438665674247607560106752257205091097473808596992',
'ops_cache-1438665674247607560106752257205091097473808596992',
'snapshot_cache-1415829711164312202009819681693899175291684651008',
'ops_cache-1415829711164312202009819681693899175291684651008',
'snapshot_cache-1392993748081016843912887106182707253109560705024'
|...]
```
{% endraw %}
Alternatively, ```ets:i()``` can be used to obtain more details about tables. Additional informations compared to ```ets:all()``` call include ```id, type, size, mem, owner```.

```erlang
> ets:i().
> id              name              type  size   mem      owner
 ----------------------------------------------------------------------------
 2031729         committed_tx      set   1      314      <0.1580.0>
 'snapshot_cache-502391187832497878132516661246222288006726811648'
 'snapshot_cache-502391187832497878132516661246222288006726811648' set   1    504    <0.1966.0>
...
```

### Table information

The [info](http://erlang.org/doc/man/ets.html#info-1) function - ```ets:info(Tab)``` - displays detailed information about table ```Tab```.

```erlang
> ets:info('ops_cache-502391187832497878132516661246222288006726811648').
>[{read_concurrency,true},
 {write_concurrency,false},
 {compressed,false},
 {memory,2922},
 {owner,<0.1966.0>},
 {heir,none},
 {name,'ops_cache-502391187832497878132516661246222288006726811648'},
 {size,1},
 {node,'antidote@127.0.0.1'},
 {named_table,true},
 {type,set},
 {keypos,1},
 {protection,protected}]
```

### Reading a table
The [tab2list](http://erlang.org/doc/man/ets.html#tab2list-1) function - ```ets:tab2list(Tab)```  - comfortably presents the table contents on screen (our particular output is explained a bit later)

{% raw %}
```erlang
> ets:tab2list('ops_cache-502391187832497878132516661246222288006726811648').
[{{my_counter,my_bucket},
  {29,50},
  29,
  {1,
   {clocksi_payload,{my_counter,my_bucket},
                    antidote_crdt_counter_pn,1,
                    {dict,1,16,16,8,80,48,
                          {[],[],[],[],[],[],[],[],[],...},
                          {{[],[],[],[],[],[],[],...}}},
                    {{'antidote@127.0.0.1',{1489,496827,67871}},
                     1489504668107078},
                    {tx_id,1489504668106333,<0.5317.0>}}},
  {2,
   {clocksi_payload,{my_counter,my_bucket},
                    antidote_crdt_counter_pn,1,
                    {dict,1,16,16,8,80,48,
                          {[],[],[],[],[],[],[],[],...},
                          {{[],[],[],[],[],[],...}}},
                    {{'antidote@127.0.0.1',{1489,496827,67871}},
                     1489504655615008},
                    {tx_id,1489504655614713,<0.5307.0>}}},
…}]
```
{% endraw %}
### Table to file
The [tab2file](http://erlang.org/doc/man/ets.html#tab2file-2) function - 
```ets:tab2file(Tab, Filename)``` - dumps table ```Tab``` to file ```Filename``` in binary format.

```erlang
> ets:tab2file('ops_cache-502391187832497878132516661246222288006726811648', 'outputFile.ets').
```

### File to table
The [file2tab](http://erlang.org/doc/man/ets.html#file2tab-2) - ```ets:file2tab(Filename, Options)``` - function reads a file produced by ```ets:tab2file``` and creates the corresponding table. 

```erlang
> ets:file2tab('outputFile.ets', [{verify, true}]).
{ok,'ops_cache-502391187832497878132516661246222288006726811648'}
```

### Understanding the contents
Supposing the following interaction with Antidote

```erlang 
CounterObj = {my_counter, antidote_crdt_counter_pn, my_bucket},
CounterVal = rpc:call(Node, antidote, read_objects, [ignore, [], [CounterObj]]),
{ok, CT}  = rpc:call(Node, antidote, update_objects, [ignore, [], [{CounterObj, increment, 1}]])
```

#### Snapshot cache contents

Contents
{% raw %}
```erlang
> ets:tab2list('snapshot_cache-502391187832497878132516661246222288006726811648').
[{{my_counter,my_bucket},
  {[{{dict,0,16,16,8,80,48,
           {[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]},
           {{[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]}}},
     {materialized_snapshot,0,0}}],
   1}}]
Ok
```
{% endraw %}
Pattern
```erlang
key, snapshot_time, 
materialized_snapshot {
	last_op_id :: op_num = 0, 
	value :: snapshot = 0
}
```

#### Operations cache contents

Contents
{% raw %}
```erlang
> ets:tab2list('ops_cache-502391187832497878132516661246222288006726811648').
[{{my_counter,my_bucket},
  {1,50},
  1,
  {1,
   {clocksi_payload,{my_counter,my_bucket},
                    antidote_crdt_counter_pn,1,
                    {dict,1,16,16,8,80,48,
                          {[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]},
                          {{[[{'antidote@127.0.0.1',{1490,186897,598677}}|
                              1490186922302506]],
                            [],[],[],[],[],[],[],[],[],[],[],[],[],[],[]}}},
                    {{'antidote@127.0.0.1',{1490,186897,598677}},
                     1490186922302997},
                    {tx_id,1490186922302506,<0.3610.0>}}},
  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}]
Ok
```
{% endraw %}

Pattern
{% raw %}
```erlang
clocksi_payload {
	key={my_counter,my_bucket}
	type=antidote_crdt_counter_pn
	op_param=1
	snapshot_time={dict,1,16,16,8,80,48, {[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]}, {{[[{'antidote@127.0.0.1',{1490,186897,598677}}|1490186922302506]],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]}}}
	commit_time {
		dcid={'antidote@127.0.0.1',{1490,186897,598677}}
		clock_time=1490186922302997
	}
	tx_id {
		local_start_time=1490186922302506, 
		server_pid=<0.3610.0>
	}
}
```
{% endraw %}

#### OpID Table

Contents
{% raw %}
```erlang
> ets:tab2list(6226140). 
[{{[502391187832497878132516661246222288006726811648],
   {'antidote@127.0.0.1',{1490,186897,598677}}},
  {op_number,
  	{'antidote@127.0.0.1',
  		{'antidote@127.0.0.1',
  			{1490,186897,598677}}},3,3}},
 {{[502391187832497878132516661246222288006726811648],
   undefined,
  {'antidote@127.0.0.1',
   		{1490,186897,598677}}},
  {op_number,
  	{'antidote@127.0.0.1',
  		{'antidote@127.0.0.1',{1490,186897,598677}}},
             1,1}}]          
```
{% endraw %}
Pattern
{% raw %}
```erlang
key_hash= 502391187832497878132516661246222288006726811648,
op_number {
		node {
      		node = 'antidote@127.0.0.1', 
    		dcid {
      			node = 'antidote@127.0.0.1'
 	     		tuple = { 1490, 186897, 598677 
   		 	}
  		}
},
bucket_op_number = 
    {op_number { 
      node {
        node = 'antidote@127.0.0.1', 
        dcid {
          node = 'antidote@127.0.0.1'
          tuple = { 1490, 186897, 598677 }
        }
      },
    global = 3, 
    local = 3
}
```
{% endraw %}
#### Committed TXs table

Contents
{% raw %}
```erlang
> ets:tab2list(2031729). 
[{{my_counter,my_bucket},1490186922302997}]
```
{% endraw %}
Pattern
{% raw %}
```erlang
key {mycounter, mybucket}
timestamp = 1490186922302997
```
{% endraw %}
### Script Usage Example
Assuming an Antidote instance is running at localhost `127.0.0.1`, registered under name ```antidote```, and your target dump directory is `./dump_dir/`
(which will be created if it doesn't exist)

**Important:** The trailing ```/``` in the dump directory's name is **necessary**. ```./dump_dir``` won't work, as it references a file according to unix conventions.


```bash
$ cache_dump.erl 'antidote@127.0.0.1', "./dump_dir/"
```


## Logs

### Internal log structure
{% include image.html file="struct_log.png" %}

### Logs in storage

#### Structure
Logs in storage only consist of ```log_operation :: log_operation()```
part, that is, ```tx_id, op_type, log_payload```

#### Log files naming
Log file names come from key hashes, meaning that ```123-456.LOG``` contains data relative to keys, where the first hashes to ```123``` and the last to ```456```, they're stored in ```$_build/default/rel/antidote/data/*.LOG``` 

#### Key space partitions
Partitions are vnodes handling key spaces, routing keys are calculated with

```erlang
-spec riak_core_util:chash_key(Key :: {any(), any()}) -> binary().
```

This function returns an integer between 0 and 2^160 - 1. The obtained integer refers to a particular position in Riak Core’s 160-bit circular key-space.


```erlang
> dc_utilities:get_all_partitions()
```
Returns a list of all partition indices in the cluster.


#### `sync_log` parameter
```sync_log```, definable in ```src/antidote.app.src``` 

`true` : local transactions will be stored on log synchronously, i.e. when the reply is sent the updates are guaranteed to be
stored to disk (this is very slow in the current logging setup)

`false` : all updates are sent to the operating system to be stored to disk (eventually), but are not guaranteed to be stored durably on disk
when the reply is sent

### Understanding the log contents

Prepare record

```erlang
{[1370157784997721485815954530671515330927436759040],
  {log_record,0,
      {op_number,
          {'antidote@127.0.0.1',{'antidote@127.0.0.1',{1490,186897,598677}}},
          574,574},
      {op_number,
          {'antidote@127.0.0.1',{'antidote@127.0.0.1',{1490,186897,598677}}},
          574,574},
      {log_operation,
          {tx_id,1490876555089336,<0.7102.0>},
          prepare,
          {prepare_log_payload,1490876555415447}}}},
````
Contents

```erlang
{[key_hash = 1370157784997721485815954530671515330927436759040],
	{type=log_record, version = 0, 
		{op_number ={node{node='antidote@127.0.0.1', dcid = {'antidote@127.0.0.1',{1490,186897,598677}}, global=574, local=574},
		{bucket_op_number = node, 
			node={node='antidote@127.0.0.1', dcid = {'antidote@127.0.0.1',{1490,186897,598677}}, global=574, local=574},
		{log_operation, 
			{tx_id, local_start_time=1490876555089336, server_pid=<0.7102.0>, 
			op_type=prepare, 
{prepare_log_payload, prepare_time=1490876555415447}}}}
```

Update record

```erlang
  {log_record,0,
      {op_number,
          {'antidote@127.0.0.1',{'antidote@127.0.0.1',{1490,186897,598677}}},
          1,1},
      {op_number,
          {'antidote@127.0.0.1',{'antidote@127.0.0.1',{1490,186897,598677}}},
          1,1},
      {log_operation,
          {tx_id,1490876399777331,<9613.5640.0>},
          update,
          {update_log_payload,
              {<<"5548">>,<<"antidote_bench_bucket">>},
              undefined,antidote_crdt_counter_pn,-1}}}}
  ```

Contents
```erlang
{log_operation, 
	{tx_id, local_start_time=1490876399777331, pid=<9613.5640.0>,
op_type=update
```

Commit record
```erlang 
 {[1370157784997721485815954530671515330927436759040],
  {log_record,0,
      {op_number,
          {'antidote@127.0.0.1',{'antidote@127.0.0.1',{1490,186897,598677}}},
          575,575},
      {op_number,
          {'antidote@127.0.0.1',{'antidote@127.0.0.1',{1490,186897,598677}}},
          575,575},
      {log_operation,
          {tx_id,1490876555089336,<0.7102.0>},
          commit,
          {commit_log_payload,
              {{'antidote@127.0.0.1',{1490,186897,598677}},1490876555487478},
              {dict,1,16,16,8,80,48,
                  {[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]},
                  {{[[{'antidote@127.0.0.1',{1490,186897,598677}}|
                      1490876555089336]],
                    [],[],[],[],[],[],[],[],[],[],[],[],[],[],[]}}}}}}}, …
--------------------------------------------------------------------------------------------------
Commit log payload pattern (the rest remains the same) 
op_type=commit, 
{commit_log_payload, 
	{commit_time={dcid={'antidote@127.0.0.1',{1490,186897,598677}}, clock_time=1490876555487478},
	snapshot_time (vectorclock) =
	      {dict,1,16,16,8,80,48,
	                  {[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]},
	                  {{[[{'antidote@127.0.0.1',{1490,186897,598677}}|
	                      1490876555089336]],
                    [],[],[],[],[],[],[],[],[],[],[],[],[],[],[]}}}}}}}, …
```

### Script Usage Example

#### Prerequisites 
- Antidote is running, logs are not empty
- Script parameters : `node_name@address dump_directory`
	- The trailing slash in `dump_directory` is required per unix conventions

#### How the script works

- Connect with a redundant connection verification
- Create, if needed, the dump directory specified in 2nd argument
- Retrieve log files handles available at the node. Note that both local and distributed log handles are fetched, however the tool only processes the local logs  
- Fetch logs accessible at the node

```erlang
disk_log:accessible_logs() -> {[LocalLog], [DistributedLog]}
disk_log:accessible_logs().
{["data/0--0",  
"data/1004782375664995756265033322492444576013453623296--1004782375664995756265033322492444576013453623296", 
"data/1027618338748291114361965898003636498195577569280--1027618338748291114361965898003636498195577569280",
"data/1050454301831586472458898473514828420377701515264--1050454301831586472458898473514828420377701515264",
"data/1073290264914881830555831049026020342559825461248--1073290264914881830555831049026020342559825461248",
"data/1096126227998177188652763624537212264741949407232--1096126227998177188652763624537212264741949407232",
	  [...]|...],
	 []}
```  

- Prepare the dump filename, which respects following syntax for facilitated differentiation: `log_dump-YEAR_MONTH_DAY-HOUR_MINUTE_SECOND.txt`
The time values are fetched with Erlang's BIF: `calendar:now_to_local_time(erlang:timestamp()).`
- The log files are treated sequentially in the alphanumerical order (i.e. as provided by `disk_log:accessible_logs()` call) and delivered following many-to-one scenario (all the logs are stored in a single dump file), in the directory specified as 2nd call argument.
	Erlang terms formatting is maintained, in order to ensure compatibility with BIF functions used to reload records into memory. Notably the `file:consult/1` and `erlang:is_record/2,3`. The former allows to parse a file and store its contents into Erlang records - assuming they're properly formatted - the latter verifies the record integrity  
	Turns out because of specific formatting used within Antidote, those BIFs do not work in this particular scenario.
- Example log contents of a prepare and commit statements

```bash
$ cat ./dump_dir/log_dump-2017_4_11-13_57_25.txt
...
{[1370157784997721485815954530671515330927436759040],
  {log_record,0,
      {op_number,
          {'antidote@127.0.0.1',{'antidote@127.0.0.1',{1490,186897,598677}}},
          574,574},
      {op_number,
          {'antidote@127.0.0.1',{'antidote@127.0.0.1',{1490,186897,598677}}},
          574,574},
      {log_operation,
          {tx_id,1490876555089336,<0.7102.0>},
          prepare,
          {prepare_log_payload,1490876555415447}}}},
 {[1370157784997721485815954530671515330927436759040],
  {log_record,0,
      {op_number,
          {'antidote@127.0.0.1',{'antidote@127.0.0.1',{1490,186897,598677}}},
          575,575},
      {op_number,
          {'antidote@127.0.0.1',{'antidote@127.0.0.1',{1490,186897,598677}}},
          575,575},
      {log_operation,
          {tx_id,1490876555089336,<0.7102.0>},
          commit,
          {commit_log_payload,
              {{'antidote@127.0.0.1',{1490,186897,598677}},1490876555487478},
              {dict,1,16,16,8,80,48,
                  {[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]},
                  {{[[{'antidote@127.0.0.1',{1490,186897,598677}}|
                      1490876555089336]],
                    [],[],[],[],[],[],[],[],[],[],[],[],[],[],[]}}}}}}}, ...
```

#### Run

```bash
$ log_dump.erl "antidote@127.0.0.1" "./dump_dir"
Connected: 'antidote@127.0.0.1'
Dir created: ./dump_dir/
Retrieved 64 local and 0 distributed logs
Processing logs from 'antidote@127.0.0.1' to "./dump_dir/log_dump-2017_4_11-13_57_25.txt"
---------------------------------------------------------------- ok
```
