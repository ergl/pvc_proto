-module(common).

%% API
-export([encode_driver_module/1,
         decode_driver_module/1]).

-export([encode_success/1,
         decode_success/1,
         encode_error/1,
         decode_error/1]).

-spec encode_driver_module(atom()) -> non_neg_integer().
encode_driver_module(ppb_rubis_driver) -> 1;
encode_driver_module(ppb_simple_driver) -> 2;
encode_driver_module(ppb_protocol_driver) -> 3.

-spec decode_driver_module(binary()) -> atom().
decode_driver_module(Bin) ->
    <<N:8, _/binary>> = Bin,
    decode_driver_module_int(N).

-spec decode_driver_module_int(non_neg_integer()) -> atom().
decode_driver_module_int(1) -> ppb_rubis_driver;
decode_driver_module_int(2) -> ppb_simple_driver;
decode_driver_module_int(3) -> ppb_protocol_driver.

-spec encode_success(atom()) -> non_neg_integer().
encode_success(_) -> 1.

-spec decode_success(non_neg_integer()) -> atom().
decode_success(_) -> ok.

%% TODO: Add ppb_protocol_driver errors
%% @doc Encode server errors as ints
-spec encode_error(atom()) -> non_neg_integer().
encode_error(user_not_found) -> 1;
encode_error(wrong_password) -> 2;
encode_error(non_unique_username) -> 3;
encode_error(timeout) -> 4;
encode_error(pvc_conflict) -> 5;
encode_error(pvc_stale_vc) -> 6;
encode_error(pvc_bad_vc) -> 7;
encode_error(_Other) -> 0.

%% TODO: Add ppb_protocol_driver errors
%% @doc Get original error types
-spec decode_error(non_neg_integer()) -> atom().
decode_error(0) -> unknown;
decode_error(1) -> user_not_found;
decode_error(2) -> wrong_password;
decode_error(3) -> non_unique_username;
decode_error(4) -> timeout;
decode_error(5) -> pvc_conflict;
decode_error(6) -> pvc_stale_vc;
decode_error(7) -> pvc_bad_vc.
