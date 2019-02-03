-module(pvc_proto).

%% API
%% Generic client - server side methods
-export([decode_client_req/1,
         encode_serv_reply/3,
         decode_serv_reply/1]).

%% @doc Generic server side decode
-spec decode_client_req(binary()) -> {atom(), atom(), #{}}.
decode_client_req(Bin) ->
    Module = common:decode_driver_module(Bin),
    {PbPtype, Decoded} = Module:from_client_dec(Bin),
    {Module, PbPtype, Decoded}.

-spec encode_serv_reply(atom(), atom(), any()) -> binary().
encode_serv_reply(Module, PbType, Result) ->
    Module:to_client_enc(PbType, Result).

-spec decode_serv_reply(binary()) -> any().
decode_serv_reply(Bin) ->
    Module = common:decode_driver_module(Bin),
    Module:from_server_dec(Bin).
