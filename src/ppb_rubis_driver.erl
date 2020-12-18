-module(ppb_rubis_driver).

-export([from_client_dec/1,
         to_client_enc/2,
         from_server_dec/1]).

%% Init
-export([preload/1]).

-define(proto_msgs, rubis_msgs).
-define(encode_msg(Type, Body), encode_raw_bits(Type, ?proto_msgs:encode_msg(Body, Type))).
-type msg() :: binary().
-export_type([msg/0]).

%%====================================================================
%% API functions
%%====================================================================

preload(Properties) ->
    ?encode_msg('RubisPreload', #{payload => term_to_binary(Properties)}).

%%====================================================================
%% Module API functions
%%====================================================================

%% @doc Generic client-side decoding
%%
%%      Uses built-in message number to decode payload
%%

-spec from_client_dec(binary()) -> {atom(), #{}}.
from_client_dec(Bin) ->
    {Type, BinMsg} = decode_raw_bits(Bin),
    {Type, decode_from_client(Type, BinMsg)}.

decode_from_client('RubisPreload', BinMsg) ->
    #{payload := BPayload} = ?proto_msgs:decode_msg(BinMsg, 'RubisPreload'),
    #{payload => binary_to_term(BPayload)}.

%% @doc Generic server-side encoding
%%
%%      First argument was the client command,
%%      so we can automatically determine the type of
%%      the reply.
%%

to_client_enc('RubisPreload', ok) ->
    ?encode_msg('RubisPreloadAck', #{}).

%% @doc Generic client side decode
from_server_dec(Bin) ->
    {Type, Msg} = decode_raw_bits(Bin),
    decode_from_server(Type, Msg).

decode_from_server('RubisPreloadAck', _) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

%% @doc Encode Protobuf msg along with msg info
-spec encode_raw_bits(atom(), binary()) -> binary().
encode_raw_bits(Type, Msg) ->
    HandlerNum = common:encode_driver_module(?MODULE),
    TypeNum = encode_msg_type(Type),
    <<HandlerNum:8, TypeNum:8, Msg/binary>>.

%% @doc Return msg type and msg from raw bits
-spec decode_raw_bits(binary()) -> {atom(), binary()}.
decode_raw_bits(Bin) ->
    <<_:8, N:8, Msg/binary>> = Bin,
    {decode_type_num(N), Msg}.

%% @doc Encode msg type as ints
-spec encode_msg_type(atom()) -> non_neg_integer().
encode_msg_type('RubisPreload') -> 1;
encode_msg_type('RubisPreloadAck') -> 2.

%% @doc Get original message type
-spec decode_type_num(non_neg_integer()) -> atom().
decode_type_num(1) -> 'RubisPreload';
decode_type_num(2) -> 'RubisPreloadAck'.
