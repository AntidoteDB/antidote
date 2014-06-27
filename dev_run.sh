make stagedevrel
./riak_test/bin/floppystore-current.sh
make && /Users/alek/floppystore/riak_test/riak_test -v -c floppystore -v -t append_list_test