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
