-module(common).

%% API
-export([encode_driver_module/1,
         decode_driver_module/1]).

-export([encode_success/1,
         decode_success/1,
         encode_error/1,
         decode_error/1,
         encode_protocol/1,
         decode_protocol/1]).

-spec encode_driver_module(atom()) -> non_neg_integer().
encode_driver_module(ppb_rubis_driver) -> 1;
encode_driver_module(ppb_simple_driver) -> 2;
encode_driver_module(ppb_protocol_driver) -> 3;
encode_driver_module(ppb_grb_driver) -> 4.

-spec decode_driver_module(binary()) -> atom().
decode_driver_module(Bin) ->
    <<N:8, _/binary>> = Bin,
    decode_driver_module_int(N).

-spec decode_driver_module_int(non_neg_integer()) -> atom().
decode_driver_module_int(1) -> ppb_rubis_driver;
decode_driver_module_int(2) -> ppb_simple_driver;
decode_driver_module_int(3) -> ppb_protocol_driver;
decode_driver_module_int(4) -> ppb_grb_driver.

-spec encode_success(atom()) -> non_neg_integer().
encode_success(_) -> 1.

-spec decode_success(non_neg_integer()) -> atom().
decode_success(_) -> ok.

%% @doc Encode server errors as ints
-spec encode_error({atom(), atom()} | atom()) -> non_neg_integer().
encode_error({rubis, user_not_found}) -> 1;
encode_error({rubis, wrong_password}) -> 2;
encode_error({rubis, non_unique_username}) -> 3;

%% Conflict on prepared
encode_error({fastpsi, pvc_conflict}) -> 1;
%% Conflict on commited
encode_error({fastpsi, pvc_stale_tx}) -> 2;
%% fastPSI: read error
encode_error({fastpsi, maxvc_bad_vc}) -> 3.

encode_error(_Other) -> 0.

%% @doc Get original error types
-spec decode_error({atom(), non_neg_integer()} | non_neg_integer()) -> atom().
decode_error(0) -> unknown;

decode_error({rubis, 1}) -> user_not_found;
decode_error({rubis, 2}) -> wrong_password;
decode_error({rubis, 3}) -> non_unique_username;

decode_error({fastpsi, 1}) -> pvc_conflict;
decode_error({fastpsi, 2}) -> pvc_stale_tx;
decode_error({fastpsi, 3}) -> maxvc_bad_vc.

%% Client coordinator protocol version
-spec encode_protocol(atom()) -> non_neg_integer().
encode_protocol(psi) -> 1;
encode_protocol(ser) -> 2;
encode_protocol(rc) -> 3;
encode_protocol(_) -> 0.

-spec decode_protocol(non_neg_integer()) -> atom().
decode_protocol(0) -> unknown;
decode_protocol(1) -> psi;
decode_protocol(2) -> ser;
decode_protocol(3) -> rc.
