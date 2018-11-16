-module(rpb_simple_driver).

-export([from_client_dec/1,
         to_client_enc/2,
         from_server_dec/1]).

-export([ping/0,
         get_ring/0,
         load/2,
         read_only/1,
         read_write/2]).

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
    );

to_client_enc(_, {request_to, Ip}) ->
    encode_raw_bits(
        'CommitResp',
        simple_msgs:encode_msg(
            #{resp => {request_to, Ip}},
            'CommitResp'
        )
    );

to_client_enc('GetRing', Ring) when is_list(Ring) ->
    BinNodes = [#{node_entry => Id, node_ip => Ip} || {Id, Ip} <- Ring],
    encode_raw_bits(
        'Ring',
        simple_msgs:encode_msg(
            #{ring => BinNodes},
            'Ring'
        )
    ).

%% @doc Generic client side decode
from_server_dec(Bin) ->
    {Type, Msg} = decode_raw_bits(Bin),
    from_server_dec(Type, Msg).

from_server_dec('CommitResp', BinMsg) ->
    Resp = maps:get(resp, simple_msgs:decode_msg(BinMsg, 'CommitResp')),
    case Resp of
        {success, Code} ->
            common:decode_success(Code);
        {error_reason, Code} ->
            {error, common:decode_error(Code)};
        {request_to, Ip} ->
            {request_to, Ip}
    end;

from_server_dec('GetRing', BinMsg) ->
    Resp = maps:get(ring, simple_msgs:decode_msg(BinMsg, 'Ring')),
    [{Id, Ip} || #{node_entry := Id, node_ip := Ip} <- Resp].

-spec ping() -> binary().
ping() ->
    Msg = simple_msgs:encode_msg(#{}, 'Ping'),
    encode_raw_bits('Ping', Msg).

-spec get_ring() -> binary().
get_ring() ->
    Msg = simple_msgs:encode_msg(#{}, 'GetRing'),
    encode_raw_bits('GetRing', Msg).

load(NumKeys, BinSize) ->
    Msg = simple_msgs:encode_msg(#{num_keys => NumKeys, bin_size => BinSize}, 'Load'),
    encode_raw_bits('Load', Msg).

-spec read_only([binary()]) -> binary().
read_only(Keys) when is_list(Keys) ->
    Msg = simple_msgs:encode_msg(#{keys => Keys}, 'ReadOnlyTx'),
    encode_raw_bits('ReadOnlyTx', Msg).

read_write(Keys, Updates) when is_list(Keys) andalso is_list(Updates) ->
    Ops = lists:map(fun({K, V}) ->
        #{key => K, value => V}
    end, Updates),
    Msg = simple_msgs:encode_msg(#{read_keys => Keys, ops => Ops}, 'ReadWriteTx'),
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
encode_msg_type('Ping') -> 4;
encode_msg_type('Load') -> 5;
encode_msg_type('GetRing') -> 6;

%% Server Responses
encode_msg_type('CommitResp') -> 3;
encode_msg_type('Ring') -> 7.

%% @doc Get original message type
-spec decode_type_num(non_neg_integer()) -> atom().

%% Client Requests
decode_type_num(1) -> 'ReadOnlyTx';
decode_type_num(2) -> 'ReadWriteTx';
decode_type_num(4) -> 'Ping';
decode_type_num(5) -> 'Load';
decode_type_num(6) -> 'GetRing';

%% Server Responses
decode_type_num(3) -> 'CommitResp';
decode_type_num(7) -> 'GetRing'.
