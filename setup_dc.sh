#!/bin/bash

./dev/dev1/bin/antidote stop
./dev/dev2/bin/antidote stop
./dev/dev3/bin/antidote stop
./dev/dev4/bin/antidote stop

sudo rm -r ./dev/dev1/data
sudo rm -r ./dev/dev2/data
sudo rm -r ./dev/dev3/data
sudo rm -r ./dev/dev4/data
sudo rm -r ./dev/dev1/log
sudo rm -r ./dev/dev2/log
sudo rm -r ./dev/dev3/log
sudo rm -r ./dev/dev4/log

sleep 2
sudo ./dev/dev1/bin/antidote start
sudo ./dev/dev2/bin/antidote start
sudo ./dev/dev3/bin/antidote start
sudo ./dev/dev4/bin/antidote start

sudo erl -pa script -name setter@127.0.0.1 -setcookie antidote -run setup_dc setup_dc_manager -run init stop 
