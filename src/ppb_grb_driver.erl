-module(ppb_grb_driver).

-export([from_client_dec/1,
         to_client_enc/2,
         from_server_dec/1]).

-export([preload/1]).

-export([connect/0,
         put_conflicts/1,
         put_direct/2,
         uniform_barrier/2,
         start_tx/2]).

%% Read / Write+Read API
-export([read_request/6,
         read_request_partition/5,
         read_operation_request/6,
         update_request/6,
         update_request_partition/5,
         read_operation_partition/5]).

%% Simple write API
-export([op_send/4]).

%% Commit API
-export([prepare_blue_node/3,
         decide_blue_node/3,
         commit_red/5]).

-define(proto_msgs, grb_msgs).
-define(encode_msg(Type, Body), encode_raw_bits(Type, ?proto_msgs:encode_msg(Body, Type))).
-type msg() :: binary().
-export_type([msg/0]).

%%====================================================================
%% API functions
%%====================================================================

-spec connect() -> msg().
connect() -> ?encode_msg('ConnectRequest', #{}).

preload(Properties) ->
    ?encode_msg('Preload', #{payload => term_to_binary(Properties)}).

-spec put_conflicts(#{binary() := binary()}) -> msg().
put_conflicts(ConflictMap) ->
    ?encode_msg('PutConflictRelations', #{payload => term_to_binary(ConflictMap)}).

-spec put_direct(non_neg_integer(), #{term() := term()}) -> msg().
put_direct(Partition, WS) ->
    ?encode_msg('PutDirect', #{partition => binary:encode_unsigned(Partition),
                               payload => term_to_binary(WS)}).

-spec uniform_barrier(non_neg_integer(), term()) -> msg().
uniform_barrier(Partition, CVC) ->
    ?encode_msg('UniformBarrier', #{client_vc => term_to_binary(CVC),
                                    partition => binary:encode_unsigned(Partition)}).

-spec start_tx(non_neg_integer(), term()) -> msg().
start_tx(Partition, CVC) ->
    ?encode_msg('StartReq', #{client_vc => term_to_binary(CVC),
                              partition => binary:encode_unsigned(Partition)}).

-spec read_request(non_neg_integer(), term(), term(), boolean(), term(), term()) -> msg().
read_request(Partition, TxId, SVC, ReadAgain, Key, Type) ->
   ?encode_msg('OpRequest', #{partition => binary:encode_unsigned(Partition),
                              transaction_id => term_to_binary(TxId),
                              snapshot_vc => term_to_binary(SVC),
                              key => term_to_binary(Key),
                              read_again => ReadAgain,
                              payload => {type, term_to_binary(Type)}}).

-spec read_operation_request(non_neg_integer(), term(), term(), boolean(), term(), term()) -> msg().
read_operation_request(Partition, TxId, SVC, ReadAgain, Key, ReadOp) ->
    ?encode_msg('OpRequest', #{partition => binary:encode_unsigned(Partition),
                               transaction_id => term_to_binary(TxId),
                               snapshot_vc => term_to_binary(SVC),
                               key => term_to_binary(Key),
                               read_again => ReadAgain,
                               payload => {read_operation, term_to_binary(ReadOp)}}).

-spec update_request(non_neg_integer(), term(), term(), boolean(), term(), term()) -> msg().
update_request(Partition, TxId, SVC, ReadAgain, Key, Update) ->
    ?encode_msg('OpRequest', #{partition => binary:encode_unsigned(Partition),
                               transaction_id => term_to_binary(TxId),
                               snapshot_vc => term_to_binary(SVC),
                               key => term_to_binary(Key),
                               read_again => ReadAgain,
                               payload => {operation, term_to_binary(Update)}}).

-spec read_request_partition(non_neg_integer(), term(), term(), boolean(), [{term(), term()}]) -> msg().
read_request_partition(Partition, TxId, SVC, ReadAgain, KeyTypes) ->
    ?encode_msg('OpRequestPartition', #{partition => binary:encode_unsigned(Partition),
                                        transaction_id => term_to_binary(TxId),
                                        snapshot_vc => term_to_binary(SVC),
                                        read_again => ReadAgain,
                                        payload => {keytypes, term_to_binary(KeyTypes)}}).

-spec read_operation_partition(non_neg_integer(), term(), term(), boolean(), [{term(), term()}]) -> msg().
read_operation_partition(Partition, TxId, SVC, ReadAgain, KeyRops) ->
    ?encode_msg('OpRequestPartition', #{partition => binary:encode_unsigned(Partition),
                                        transaction_id => term_to_binary(TxId),
                                        snapshot_vc => term_to_binary(SVC),
                                        read_again => ReadAgain,
                                        payload => {keyreadops, term_to_binary(KeyRops)}}).

-spec update_request_partition(non_neg_integer(), term(), term(), boolean(), [{term(), term()}]) -> msg().
update_request_partition(Partition, TxId, SVC, ReadAgain, KeyOps) ->
    ?encode_msg('OpRequestPartition', #{partition => binary:encode_unsigned(Partition),
                                        transaction_id => term_to_binary(TxId),
                                        snapshot_vc => term_to_binary(SVC),
                                        read_again => ReadAgain,
                                        payload => {keyops, term_to_binary(KeyOps)}}).

-spec op_send(non_neg_integer(), term(), term(), term()) -> msg().
op_send(Partition, TxId, Key, Op) ->
    ?encode_msg('OpSend', #{partition => binary:encode_unsigned(Partition),
                            transaction_id => term_to_binary(TxId),
                            key => term_to_binary(Key),
                            operation => term_to_binary(Op)}).

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

-spec commit_red(term(), term(), binary(), term(), [{non_neg_integer(), term(), term()}]) -> msg().
commit_red(Partition, TxId, Label, SVC, Prepares) ->
    BinPrepares = lists:map(fun term_to_binary/1, Prepares),
    ?encode_msg('CommitRed', #{partition => binary:encode_unsigned(Partition),
                               transaction_id => term_to_binary(TxId),
                               snapshot_vc => term_to_binary(SVC),
                               transaction_label => Label,
                               prepares => BinPrepares}).

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
        key := BKey,
        payload := Payload
    } = ?proto_msgs:decode_msg(Msg, 'OpRequest'),
    M1 = maps:remove(
        payload,
        M0#{partition := binary:decode_unsigned(PB),
            transaction_id := binary_to_term(PTxId),
            key := binary_to_term(BKey),
            snapshot_vc := binary_to_term(PSVC)}
    ),
    case Payload of
        {type, Typ} ->
            M1#{type => binary_to_term(Typ)};
        {operation, Op} ->
            M1#{operation => binary_to_term(Op)};
        {read_operation, ROp} ->
            M1#{read_operation => binary_to_term(ROp)}
    end;

decode_from_client('OpRequestPartition', Msg) ->
    M0 = #{
        partition := BPartition,
        transaction_id := PTxId,
        snapshot_vc := PSVC,
        payload := Payload
    } = ?proto_msgs:decode_msg(Msg, 'OpRequestPartition'),
    M1 = M0#{partition := binary:decode_unsigned(BPartition),
             snapshot_vc := binary_to_term(PSVC),
             transaction_id := binary_to_term(PTxId)},
    M3 = maps:remove(payload, M1),
    case Payload of
        {keytypes, BKeyTypes} ->
            M3#{reads => binary_to_term(BKeyTypes)};
        {keyreadops, BKeyRops} ->
            M3#{read_ops => binary_to_term(BKeyRops)};
        {keyops, BKeyOps} ->
            M3#{operations => binary_to_term(BKeyOps)}
    end;

decode_from_client('OpSend', Msg) ->
    M0 = #{
        partition := BPartition,
        transaction_id := PTxId,
        key := BKey,
        operation := BOp
    } = ?proto_msgs:decode_msg(Msg, 'OpSend'),
    M0#{partition := binary:decode_unsigned(BPartition),
        transaction_id := binary_to_term(PTxId),
        key := binary_to_term(BKey),
        operation := binary_to_term(BOp)};

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
                 (transaction_label, V) -> V;
                 (_, V) -> binary_to_term(V) end, Map);

decode_from_client('PutConflictRelations', Msg) ->
    #{payload := P} = ?proto_msgs:decode_msg(Msg, 'PutConflictRelations'),
    #{payload => binary_to_term(P)};

decode_from_client('PutDirect', Msg) ->
    #{partition := PB, payload := PLB} = ?proto_msgs:decode_msg(Msg, 'PutDirect'),
    #{partition => binary:decode_unsigned(PB), payload => term_to_binary(PLB)};

decode_from_client('Preload', BinMsg) ->
    #{payload := BPayload} = ?proto_msgs:decode_msg(BinMsg, 'Preload'),
    #{payload => binary_to_term(BPayload)};

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

to_client_enc('Preload', ok) ->
    ?encode_msg('PreloadAck', #{});

to_client_enc('PutDirect', ok) ->
    ?encode_msg('PutDirectAck', #{});

to_client_enc('StartReq', SVC) ->
    ?encode_msg('StartReturn', #{snapshot_vc => term_to_binary(SVC)});

to_client_enc('UniformBarrier', ok) ->
    ?encode_msg('UniformResp', #{});

to_client_enc('OpRequest', {ok, Val}) when is_binary(Val) ->
    ?encode_msg('OpReturn', #{value => Val, transform => false});

to_client_enc('OpRequest', {ok, Val}) ->
    ?encode_msg('OpReturn', #{value => term_to_binary(Val), transform => true});

to_client_enc('OpRequestPartition', Responses) ->
    ?encode_msg('OpReturnPartition', #{payload => term_to_binary(Responses)});

to_client_enc('OpSend', ok) ->
    ?encode_msg('OpSendAck', #{});

to_client_enc('PrepareBlueNode', Votes) ->
    ?encode_msg('BlueVoteBatch', #{votes => [encode_blue_prepare(V) || V <- Votes]});

to_client_enc('CommitRed', {ok, CommitVC}) ->
    ?encode_msg('CommitRedReturn', #{resp => {commit_vc, term_to_binary(CommitVC)}});

to_client_enc('CommitRed', {abort, Reason}) ->
    ?encode_msg('CommitRedReturn', #{resp => {abort_reason, common:encode_error({grb, Reason})}});

to_client_enc('PutConflictRelations', ok) ->
    ?encode_msg('PutConflictRelationsAck', #{}).

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

decode_from_server('PreloadAck', _) ->
    ok;

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

decode_from_server('OpReturnPartition', BinMsg) ->
    #{payload := Payload} = ?proto_msgs:decode_msg(BinMsg, 'OpReturnPartition'),
    {ok, binary_to_term(Payload)};

decode_from_server('OpSendAck', _) ->
    ok;

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
    end;

decode_from_server('PutConflictRelationsAck', _) ->
    ok;

decode_from_server('PutDirectAck', _) ->
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
encode_msg_type('CommitRedReturn') -> 13;
encode_msg_type('PutConflictRelations') -> 14;
encode_msg_type('PutConflictRelationsAck') -> 15;
encode_msg_type('PutDirect') -> 16;
encode_msg_type('PutDirectAck') -> 17;
encode_msg_type('OpRequestPartition') -> 18;
encode_msg_type('OpReturnPartition') -> 19;
encode_msg_type('OpSend') -> 20;
encode_msg_type('OpSendAck') -> 21;
encode_msg_type('Preload') -> 22;
encode_msg_type('PreloadAck') -> 23.

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
decode_type_num(13) -> 'CommitRedReturn';
decode_type_num(14) -> 'PutConflictRelations';
decode_type_num(15) -> 'PutConflictRelationsAck';
decode_type_num(16) -> 'PutDirect';
decode_type_num(17) -> 'PutDirectAck';
decode_type_num(18) -> 'OpRequestPartition';
decode_type_num(19) -> 'OpReturnPartition';
decode_type_num(20) -> 'OpSend';
decode_type_num(21) -> 'OpSendAck';
decode_type_num(22) -> 'Preload';
decode_type_num(23) -> 'PreloadAck'.
