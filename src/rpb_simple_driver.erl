-module(rpb_simple_driver).

-export([from_client_dec/1,
         to_client_enc/2,
         from_server_dec/1]).

-export([read_only/1,
         read_write/1]).

-spec from_client_dec(binary()) -> {atom(), #{}}.
from_client_dec(Bin) ->
    {Type, BinMsg} = decode_raw_bits(Bin),
    {Type, simple_msgs:decode_msg(BinMsg, Type)}.

to_client_enc(_, ok) ->
    encode_raw_bits(
        'CommitResp',
        simple_msgs:encode_msg(
            #{resp => {success, common:encode_success(ok)}},
            'CommitResp'
        )
    );

to_client_enc(_, {error, Reason}) ->
    encode_raw_bits(
        'CommitResp',
        simple_msgs:encode_msg(
            #{resp => {error_reason, common:encode_error(Reason)}},
            'CommitResp'
        )
    ).

%% @doc Generic client side decode
from_server_dec(Bin) ->
    {'CommitResp', BinMsg} = decode_raw_bits(Bin),
    Resp = maps:get(resp, simple_msgs:decode_msg(BinMsg, 'CommitResp')),
    case Resp of
        {success, Code} ->
            common:decode_success(Code);
        {error_reason, Code} ->
            {error, common:decode_error(Code)}
    end.

-spec read_only([binary()]) -> binary().
read_only(Keys) when is_list(Keys) ->
    Msg = simple_msgs:encode_msg(#{keys => Keys}, 'ReadOnlyTx'),
    encode_raw_bits('ReadOnlyTx', Msg).

read_write(Updates) when is_list(Updates) ->
    Ops = lists:map(fun({K, V}) ->
        #{key => K, value => V}
    end, Updates),
    Msg = simple_msgs:encode_msg(#{ops => Ops}, 'ReadWriteTx'),
    encode_raw_bits('ReadWriteTx', Msg).

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

%% Client Requests
-spec encode_msg_type(atom()) -> non_neg_integer().
encode_msg_type('ReadOnlyTx') -> 1;
encode_msg_type('ReadWriteTx') -> 2;

%% Server Responses
encode_msg_type('CommitResp') -> 3.

%% @doc Get original message type
-spec decode_type_num(non_neg_integer()) -> atom().

%% Client Requests
decode_type_num(1) -> 'ReadOnlyTx';
decode_type_num(2) -> 'ReadWriteTx';

%% Server Responses
decode_type_num(3) -> 'CommitResp'.
