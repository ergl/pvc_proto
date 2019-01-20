-module(rpb_simple_driver).

%% pb driver callbacks
-export([from_client_dec/1,
         to_client_enc/2,
         from_server_dec/1]).

%% Timed messages
-export([timed_noop/0,
         timed_ping/0,
         timed_read/1]).

%% Normal messages
-export([partitions/0,
         load/2,
         read_only/1,
         read_write/2]).

%% PVC <-> PVC messages
-export([remote_read/4]).

-type msg() :: binary().
-export_type([msg/0]).

%%%%%%%%%%%%%%%%%%%%% Messages %%%%%%%%%%%%%%%%%%%%%

-spec timed_noop() -> msg().
timed_noop() ->
    encode_pb_msg('ByteReq', #{tag => no_op}).

-spec timed_ping() -> msg().
timed_ping() ->
    encode_pb_msg('ByteReq', #{tag => ping}).

-spec timed_read(binary()) -> msg().
timed_read(Key) when is_binary(Key) ->
    encode_pb_msg('TimedRead', #{key => Key}).

-spec partitions() -> msg().
partitions() ->
    encode_pb_msg('ByteReq', #{tag => ring}).

-spec load(non_neg_integer(), non_neg_integer()) -> msg().
load(NumKeys, BinSize) ->
    encode_pb_msg('Load', #{num_keys => NumKeys, bin_size => BinSize}).

-spec read_only([binary()]) -> msg().
read_only(Keys) when is_list(Keys) ->
    encode_pb_msg('ReadOnlyTx', #{keys => Keys}).

-spec read_write([binary()], [{binary(), binary()}]) -> msg().
read_write(Keys, Updates) when is_list(Keys) andalso is_list(Updates) ->
    Ops = lists:map(fun({K, V}) -> #{key => K, value => V} end, Updates),
    encode_pb_msg('ReadWriteTx', #{read_keys => Keys, ops => Ops}).

-spec remote_read(integer(), term(), [any()], term()) -> msg().
remote_read(Partition, Key, HasRead, VCAggr) when is_binary(Key) ->
    encode_pb_msg('RemoteRead', #{partition => term_to_binary(Partition),
                                  key => Key,
                                  has_read => term_to_binary(HasRead),
                                  vc_aggr => term_to_binary(VCAggr)});

remote_read(Partition, Key, HasRead, VCAggr) ->
    encode_pb_msg('RemoteRead', #{partition => term_to_binary(Partition),
                                  key => term_to_binary(Key),
                                  has_read => term_to_binary(HasRead),
                                  vc_aggr => term_to_binary(VCAggr)}).

%%%%%%%%%%%%%%%%%%%%% Callbacks %%%%%%%%%%%%%%%%%%%%%

%% @doc Generic server side decode
-spec from_client_dec(msg()) -> {atom(), #{}}.
from_client_dec(Bin) ->
    {Type, BinMsg} = decode_raw_bits(Bin),
    {Type, from_client_dec(Type, BinMsg)}.

from_client_dec('RemoteRead', Msg) ->
    Map = simple_msgs:decode_msg(Msg, 'RemoteRead'),
    Map#{partition := binary_to_term(maps:get(partition, Map)),
         has_read := binary_to_term(maps:get(has_read, Map)),
         vc_aggr := binary_to_term(maps:get(vc_aggr, Map))};

from_client_dec(Type, BinMsg) ->
    simple_msgs:decode_msg(BinMsg, Type).

%% @doc Generic server side encode
-spec to_client_enc(atom(), term()) -> msg().
to_client_enc('ByteReq', {Tag, Term}) ->
    encode_pb_msg('ByteResp', #{tag => Tag, payload => term_to_binary(Term)});

to_client_enc('TimedRead', {error, Reason}) ->
    encode_pb_msg('TimedReadResp', #{resp => {error_reason, common:encode_error(Reason)}});

to_client_enc('TimedRead', {ok, Term}) ->
    encode_pb_msg('TimedReadResp', #{resp => {payload, term_to_binary(Term)}});

to_client_enc('RemoteRead', {error, maxvc_bad_vc}) ->
    encode_pb_msg('RemoteReadResp', #{resp => {error_reason, common:encode_error(maxvc_bad_vc)}});

to_client_enc('RemoteRead', {ok, Payload}) ->
    encode_pb_msg('RemoteReadResp', #{resp => {payload, term_to_binary(Payload)}});

to_client_enc(_, ok) ->
    encode_pb_msg('CommitResp', #{resp => {success, common:encode_success(ok)}});

to_client_enc(_, {error, Reason}) ->
    encode_pb_msg('CommitResp', #{resp => {error_reason, common:encode_error(Reason)}}).

%% @doc Generic client side decode
from_server_dec(Bin) ->
    {Type, Msg} = decode_raw_bits(Bin),
    decode_from_server(Type, Msg).

decode_from_server('ByteResp', BinMsg) ->
    #{tag := Tag, payload := Payload} = simple_msgs:decode_msg(BinMsg, 'ByteResp'),
    {Tag, binary_to_term(Payload)};

decode_from_server('TimedReadResp', BinMsg) ->
    Resp = maps:get(resp, simple_msgs:decode_msg(BinMsg, 'TimedReadResp')),
    case Resp of
        {error_reason, Code} ->
            {error, common:decode_error(Code)};
        {payload, Payload} ->
            {ok, binary_to_term(Payload)}
    end;

decode_from_server('RemoteReadResp', BinMsg) ->
    Resp = maps:get(resp, simple_msgs:decode_msg(BinMsg, 'RemoteReadResp')),
    case Resp of
        {error_reason, Code} ->
            {error, common:decode_error(Code)};
        {payload, Payload} ->
            {ok, binary_to_term(Payload)}
    end;

decode_from_server('CommitResp', BinMsg) ->
    Resp = maps:get(resp, simple_msgs:decode_msg(BinMsg, 'CommitResp')),
    case Resp of
        {success, Code} ->
            common:decode_success(Code);
        {error_reason, Code} ->
            {error, common:decode_error(Code)}
    end.

%%%%%%%%%%%%%%%%%%%%% Util %%%%%%%%%%%%%%%%%%%%%

%% @doc Encode Protobuf msg along with msg info
-spec encode_pb_msg(atom(), any()) -> msg().
encode_pb_msg(Type, Payload) ->
    encode_raw_bits(Type, simple_msgs:encode_msg(Payload, Type)).

%% @doc Prepend handler and msg type info
-spec encode_raw_bits(atom(), binary()) -> binary().
encode_raw_bits(Type, Msg) ->
    HandlerNum = common:encode_driver_module(?MODULE),
    TypeNum = encode_msg_type(Type),
    <<HandlerNum:8, TypeNum:8, Msg/binary>>.

%% @doc Return msg type and msg from raw bits
-spec decode_raw_bits(msg()) -> {atom(), binary()}.
decode_raw_bits(Bin) ->
    <<_:8, N:8, Msg/binary>> = Bin,
    {decode_type_num(N), Msg}.

%% @doc Encode msg type as ints

%% Client Requests
-spec encode_msg_type(atom()) -> non_neg_integer().
encode_msg_type('ByteReq') -> 1;
encode_msg_type('TimedRead') -> 2;
encode_msg_type('Load') -> 3;
encode_msg_type('ReadOnlyTx') -> 4;
encode_msg_type('ReadWriteTx') -> 5;
encode_msg_type('RemoteRead') -> 6;

%% Server Responses
encode_msg_type('ByteResp') -> 7;
encode_msg_type('TimedReadResp') -> 8;
encode_msg_type('RemoteReadResp') -> 9;
encode_msg_type('CommitResp') -> 10.

%% @doc Get original message type
-spec decode_type_num(non_neg_integer()) -> atom().

%% Client Requests
decode_type_num(1) -> 'ByteReq';
decode_type_num(2) -> 'TimedRead';
decode_type_num(3) -> 'Load';
decode_type_num(4) -> 'ReadOnlyTx';
decode_type_num(5) -> 'ReadWriteTx';
decode_type_num(6) -> 'RemoteRead';

%% Server Responses
decode_type_num(7) -> 'ByteResp';
decode_type_num(8) -> 'TimedReadResp';
decode_type_num(9) -> 'RemoteReadResp';
decode_type_num(10) -> 'CommitResp'.
