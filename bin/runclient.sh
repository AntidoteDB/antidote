ROOT=../
cd $ROOT
rel/antidote/bin/antidote stop
rm -rf rel/antidote/data/
make rel
rel/antidote/bin/antidote start
sleep 5
cd antidoteclient/ebin
erl -name 'client@127.0.0.1' -cookie 'antidote' -run walletclient start a b -run init stop -noshell
cd ../
../rel/antidote/bin/antidote stop
