-module(ppb_grb_driver).

-export([from_client_dec/1,
         to_client_enc/2,
         from_server_dec/1]).

-export([connect/0,
         uniform_barrier/2,
         start_tx/2,
         read_request/5,
         update_request/6,
         prepare_blue_node/3,
         decide_blue_node/3,
         commit_red/4]).

-define(proto_msgs, grb_msgs).
-define(encode_msg(Type, Body), encode_raw_bits(Type, ?proto_msgs:encode_msg(Body, Type))).
-type msg() :: binary().
-export_type([msg/0]).

%%====================================================================
%% API functions
%%====================================================================

-spec connect() -> msg().
connect() -> ?encode_msg('ConnectRequest', #{}).

-spec uniform_barrier(non_neg_integer(), term()) -> msg().
uniform_barrier(Partition, CVC) ->
    ?encode_msg('UniformBarrier', #{client_vc => term_to_binary(CVC),
                                    partition => binary:encode_unsigned(Partition)}).

-spec start_tx(non_neg_integer(), term()) -> msg().
start_tx(Partition, CVC) ->
    ?encode_msg('StartReq', #{client_vc => term_to_binary(CVC),
                              partition => binary:encode_unsigned(Partition)}).

-spec read_request(non_neg_integer(), term(), term(), boolean(), binary()) -> msg().
read_request(Partition, TxId, SVC, ReadAgain, Key) ->
   ?encode_msg('OpRequest', #{partition => binary:encode_unsigned(Partition),
                              transaction_id => term_to_binary(TxId),
                              snapshot_vc => term_to_binary(SVC),
                              key => Key,
                              read_again => ReadAgain}).

-spec update_request(non_neg_integer(), term(), term(), boolean(), binary(), term()) -> msg().
update_request(Partition, TxId, SVC, ReadAgain, Key, Update) ->
    ?encode_msg('OpRequest', #{partition => binary:encode_unsigned(Partition),
                               transaction_id => term_to_binary(TxId),
                               snapshot_vc => term_to_binary(SVC),
                               key => Key,
                               read_again => ReadAgain,
                               operation => term_to_binary(Update)}).

-spec prepare_blue_node(term(), term(), [non_neg_integer()]) -> msg().
prepare_blue_node(TxId, SVC, Partitions) ->
    PBinary = [binary:encode_unsigned(P) || P <- Partitions],
    ?encode_msg('PrepareBlueNode', #{transaction_id => term_to_binary(TxId),
                                     snapshot_vc => term_to_binary(SVC),
                                     partitions => PBinary}).

-spec decide_blue_node(term(), [non_neg_integer()], term()) -> msg().
decide_blue_node(TxId, Partitions, CommitVC) ->
    PBinary = [binary:encode_unsigned(P) || P <- Partitions],
    ?encode_msg('DecideBlueNode', #{transaction_id => term_to_binary(TxId),
                                    partitions => PBinary,
                                    commit_vc => term_to_binary(CommitVC)}).

-spec commit_red(term(), term(), term(), [{non_neg_integer(), term(), term()}]) -> msg().
commit_red(Partition, TxId, SVC, Prepares) ->
    BinPrepares = lists:map(fun term_to_binary/1, Prepares),
    ?encode_msg('CommitRed', #{transaction_id => term_to_binary(TxId),
                               snapshot_vc => term_to_binary(SVC),
                               prepares => BinPrepares,
                               partition => binary:encode_unsigned(Partition)}).

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

decode_from_client('OpRequest', Msg) ->
    M0 = #{
        partition := PB,
        transaction_id := PTxId,
        snapshot_vc := PSVC,
        operation := POp
    } = ?proto_msgs:decode_msg(Msg, 'OpRequest'),
    M1 = M0#{partition := binary:decode_unsigned(PB),
             transaction_id := binary_to_term(PTxId),
             snapshot_vc := binary_to_term(PSVC)},
    case POp of
        <<>> ->
            maps:remove(operation, M1);
        Other ->
            M1#{operation := binary_to_term(Other)}
    end;

decode_from_client('PrepareBlueNode', Msg) ->
    Map = ?proto_msgs:decode_msg(Msg, 'PrepareBlueNode'),
    maps:map(fun(partitions, V) -> [binary:decode_unsigned(P) || P <- V];
                 (_, V) -> binary_to_term(V) end, Map);

decode_from_client('DecideBlueNode', Msg) ->
    Map = ?proto_msgs:decode_msg(Msg, 'DecideBlueNode'),
    maps:map(fun(partitions, V) -> [binary:decode_unsigned(P) || P <- V];
                 (_, V) -> binary_to_term(V) end, Map);

decode_from_client('CommitRed', Msg) ->
    Map = ?proto_msgs:decode_msg(Msg, 'CommitRed'),
    maps:map(fun(prepares, V) -> lists:map(fun binary_to_term/1, V);
                 (partition, V) -> binary:decode_unsigned(V);
                 (_, V) -> binary_to_term(V) end, Map);

decode_from_client(Type, Msg) when Type =:= 'UniformBarrier' orelse Type =:= 'StartReq' ->
    Map = ?proto_msgs:decode_msg(Msg, Type),
    maps:map(fun(client_vc, V) -> binary_to_term(V);
                (partition, P) -> binary:decode_unsigned(P) end, Map);

decode_from_client(Type, BinMsg) ->
    ?proto_msgs:decode_msg(BinMsg, Type).

%% @doc Generic server-side encoding
%%
%%      First argument was the client command,
%%      so we can automatically determine the type of
%%      the reply.
%%

to_client_enc('ConnectRequest', {ok, ReplicaID, NumPartitions, Ring}) ->
    ?encode_msg('ConnectResponse',
                #{num_partitions => NumPartitions,
                  ring_payload => term_to_binary(Ring),
                  replica_id => term_to_binary(ReplicaID)});

to_client_enc('StartReq', SVC) ->
    ?encode_msg('StartReturn', #{snapshot_vc => term_to_binary(SVC)});

to_client_enc('UniformBarrier', ok) ->
    ?encode_msg('UniformResp', #{});

to_client_enc('OpRequest', {ok, Val}) when is_binary(Val) ->
    ?encode_msg('OpReturn', #{value => Val, transform => false});

to_client_enc('OpRequest', {ok, Val}) ->
    ?encode_msg('OpReturn', #{value => term_to_binary(Val), transform => true});

to_client_enc('PrepareBlueNode', Votes) ->
    ?encode_msg('BlueVoteBatch', #{votes => [encode_blue_prepare(V) || V <- Votes]});

to_client_enc('CommitRed', {ok, CommitVC}) ->
    ?encode_msg('CommitRedReturn', #{resp => {commit_vc, term_to_binary(CommitVC)}});

to_client_enc('CommitRed', {abort, Reason}) ->
    ?encode_msg('CommitRedReturn', #{resp => {abort_reason, common:encode_error({grb, Reason})}}).

encode_blue_prepare({ok, From, SeqNumber}) ->
    #{partition => binary:encode_unsigned(From), prepare_time => SeqNumber}.

%% @doc Generic client side decode
from_server_dec(Bin) ->
    {Type, Msg} = decode_raw_bits(Bin),
    decode_from_server(Type, Msg).

decode_from_server('ConnectResponse', BinMsg) ->
    #{num_partitions := N,
      ring_payload := Bytes,
      replica_id := BinId} = ?proto_msgs:decode_msg(BinMsg, 'ConnectResponse'),
    {ok, binary_to_term(BinId), N, binary_to_term(Bytes)};

decode_from_server('UniformResp', _) ->
    ok;

decode_from_server('StartReturn', BinMsg) ->
    #{snapshot_vc := BinVC} = ?proto_msgs:decode_msg(BinMsg, 'StartReturn'),
    {ok, binary_to_term(BinVC)};

decode_from_server('OpReturn', BinMsg) ->
    #{value := Value, transform := T} = ?proto_msgs:decode_msg(BinMsg, 'OpReturn'),
    if
        T ->
            {ok, binary_to_term(Value)};
        true ->
            {ok, Value}
    end;

decode_from_server('BlueVoteBatch', BinMsg) ->
    #{votes := Votes} = ?proto_msgs:decode_msg(BinMsg, 'BlueVoteBatch'),
    [ {ok, binary:decode_unsigned(P), PT} || #{partition := P, prepare_time := PT} <- Votes];

decode_from_server('CommitRedReturn', BinMsg) ->
    Resp = maps:get(resp, ?proto_msgs:decode_msg(BinMsg, 'CommitRedReturn')),
    case Resp of
        {abort_reason, Code} ->
            {abort, common:decode_error({grb, Code})};
        {commit_vc, BinVC} ->
            {ok, binary_to_term(BinVC)}
    end.

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
encode_msg_type('ConnectRequest') -> 1;
encode_msg_type('ConnectResponse') -> 2;
encode_msg_type('UniformBarrier') -> 3;
encode_msg_type('UniformResp') -> 4;
encode_msg_type('StartReq') -> 5;
encode_msg_type('StartReturn') -> 6;
encode_msg_type('OpRequest') -> 7;
encode_msg_type('OpReturn') -> 8;
encode_msg_type('PrepareBlueNode') -> 9;
encode_msg_type('BlueVoteBatch') -> 10;
encode_msg_type('DecideBlueNode') -> 11;
encode_msg_type('CommitRed') -> 12;
encode_msg_type('CommitRedReturn') -> 13.

%% @doc Get original message type
-spec decode_type_num(non_neg_integer()) -> atom().
decode_type_num( 1) -> 'ConnectRequest';
decode_type_num( 2) -> 'ConnectResponse';
decode_type_num( 3) -> 'UniformBarrier';
decode_type_num( 4) -> 'UniformResp';
decode_type_num( 5) -> 'StartReq';
decode_type_num( 6) -> 'StartReturn';
decode_type_num( 7) -> 'OpRequest';
decode_type_num( 8) -> 'OpReturn';
decode_type_num( 9) -> 'PrepareBlueNode';
decode_type_num(10) -> 'BlueVoteBatch';
decode_type_num(11) -> 'DecideBlueNode';
decode_type_num(12) -> 'CommitRed';
decode_type_num(13) -> 'CommitRedReturn'.
