WalletApp: App that uses floppystore
======================================

walletapp
---------------------

This app provides services to credit and debit money, and buy and spend vouchers. Inorder to use this application

1. Start a floppystore node at "floppy@127.0.0.1"
2. Start erlang shell: erl -name NODE@HOST -cookie COOKIE
3. Call methods in walletapp: credit, debit, getbalance, buyvoucher, usevoucher, getvouchers

or

Use script runclient.sh 


walletclient
----------------------

A test for walletapp, which could be initiated by walletclient:run/2.

runclient.sh
----------------------

This script startup a single node floppystore cluster and runs the walletclient


