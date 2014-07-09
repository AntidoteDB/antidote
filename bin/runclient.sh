ROOT=../
cd $ROOT
rel/floppy/bin/floppy stop
rm -rf rel/floppy/data/
make rel
rel/floppy/bin/floppy start
sleep 5
cd floppyclient/ebin
erl -name 'client@127.0.0.1' -cookie 'floppy' -run walletclient start a b -run init stop -noshell
cd ../
../rel/floppy/bin/floppy stop
