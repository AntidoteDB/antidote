-ifndef(FPBINCREMENTREQ_PB_H).
-define(FPBINCREMENTREQ_PB_H, true).
-record(fpbincrementreq, {
    key = erlang:error({required, key}),
    amount = erlang:error({required, amount})
}).
-endif.

-ifndef(FPBDECREMENTREQ_PB_H).
-define(FPBDECREMENTREQ_PB_H, true).
-record(fpbdecrementreq, {
    key = erlang:error({required, key}),
    amount = erlang:error({required, amount})
}).
-endif.

-ifndef(FPBGETCOUNTERREQ_PB_H).
-define(FPBGETCOUNTERREQ_PB_H, true).
-record(fpbgetcounterreq, {
    key = erlang:error({required, key})
}).
-endif.

-ifndef(FPBGETCOUNTERRESP_PB_H).
-define(FPBGETCOUNTERRESP_PB_H, true).
-record(fpbgetcounterresp, {
    value = erlang:error({required, value})
}).
-endif.

-ifndef(FPBOPERATIONRESP_PB_H).
-define(FPBOPERATIONRESP_PB_H, true).
-record(fpboperationresp, {
    success = erlang:error({required, success})
}).
-endif.

-ifndef(FPBSETUPDATEREQ_PB_H).
-define(FPBSETUPDATEREQ_PB_H, true).
-record(fpbsetupdatereq, {
    key = erlang:error({required, key}),
    adds = [],
    rems = []
}).
-endif.

-ifndef(FPBGETSETREQ_PB_H).
-define(FPBGETSETREQ_PB_H, true).
-record(fpbgetsetreq, {
    key = erlang:error({required, key})
}).
-endif.

-ifndef(FPBGETSETRESP_PB_H).
-define(FPBGETSETRESP_PB_H, true).
-record(fpbgetsetresp, {
    value = erlang:error({required, value})
}).
-endif.

-ifndef(FPBATOMICUPDATETXNOP_PB_H).
-define(FPBATOMICUPDATETXNOP_PB_H, true).
-record(fpbatomicupdatetxnop, {
    counterinc,
    counterdec,
    setupdate
}).
-endif.

-ifndef(FPBATOMICUPDATETXNREQ_PB_H).
-define(FPBATOMICUPDATETXNREQ_PB_H, true).
-record(fpbatomicupdatetxnreq, {
    clock,
    ops = []
}).
-endif.

-ifndef(FPBATOMICUPDATETXNRESP_PB_H).
-define(FPBATOMICUPDATETXNRESP_PB_H, true).
-record(fpbatomicupdatetxnresp, {
    success = erlang:error({required, success}),
    clock
}).
-endif.

-ifndef(FPBSNAPSHOTREADTXNOP_PB_H).
-define(FPBSNAPSHOTREADTXNOP_PB_H, true).
-record(fpbsnapshotreadtxnop, {
    counter,
    set
}).
-endif.

-ifndef(FPBSNAPSHOTREADTXNREQ_PB_H).
-define(FPBSNAPSHOTREADTXNREQ_PB_H, true).
-record(fpbsnapshotreadtxnreq, {
    clock,
    ops = []
}).
-endif.

-ifndef(FPBSNAPSHOTREADTXNRESPVALUE_PB_H).
-define(FPBSNAPSHOTREADTXNRESPVALUE_PB_H, true).
-record(fpbsnapshotreadtxnrespvalue, {
    key = erlang:error({required, key}),
    counter,
    set
}).
-endif.

-ifndef(FPBSNAPSHOTREADTXNRESP_PB_H).
-define(FPBSNAPSHOTREADTXNRESP_PB_H, true).
-record(fpbsnapshotreadtxnresp, {
    success = erlang:error({required, success}),
    clock,
    results = []
}).
-endif.

