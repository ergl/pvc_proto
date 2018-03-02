-module(rubis_proto).

-export([peek_msg_type/1]).

-export([decode_request/1,
         encode_reply/2,
         decode_reply/2]).

%% Client side methods
-export([put_region/1,
         put_category/1,
         auth_user/2,
         browse_categories/0,
         browse_regions/0,
         search_by_category/1]).

-spec peek_msg_type(binary()) -> atom().
peek_msg_type(Bin) ->
    {Type, _} = decode_raw_bits(Bin),
    Type.

%% TODO(borja): Implement the rest

%% @doc Generic server side decode
decode_request(Bin) ->
    {Type, Msg} = decode_raw_bits(Bin),
    rubis_pb:decode_msg(Msg, Type).

%% @doc Generic client side decode
%%
%%      First argument is the original request
decode_reply('PutRegion', Msg) ->
    dec_resp('PutRegionResp', region_id, Msg);

decode_reply('PutCategory', Msg) ->
    dec_resp('PutCategoryResp', category_id, Msg);

decode_reply('AuthUser', Msg) ->
    dec_resp('AuthUserResp', user_id, Msg);

decode_reply('BrowseCategories', Msg) ->
    dec_resp('BrowseCategoriesResp', category_names, Msg);

decode_reply('BrowseRegions', Msg) ->
    dec_resp('BrowseRegionsResp', region_names, Msg);

decode_reply('SearchByCategory', Msg) ->
    dec_resp('SearchByCategoryResp', items, Msg).

%% @doc Generic client side encode
%%
%%      First argument is the original request
%%      Replies don't need to be wrapped in message identifiers
encode_reply('PutRegion', Resp) ->
    enc_resp('PutRegionResp', region_id, Resp);

encode_reply('PutCategory', Resp) ->
    enc_resp('PutCategoryResp', category_id, Resp);

encode_reply('AuthUser', Resp) ->
    enc_resp('AuthUserResp', user_id, Resp);

encode_reply('BrowseCategories', Resp) ->
    enc_resp('BrowseCategoriesResp', category_names, Resp);

encode_reply('BrowseRegions', Resp) ->
    enc_resp('BrowseRegionsResp', region_names, Resp);

encode_reply('SearchByCategory', Resp) ->
    enc_resp('SearchByCategoryResp', items, Resp).

put_region(RegionName) when is_binary(RegionName) ->
    Msg = rubis_pb:encode_msg(#{region_name => RegionName}, 'PutRegion'),
    encode_raw_bits('PutRegion', Msg).

put_category(CategoryName) when is_binary(CategoryName) ->
    Msg = rubis_pb:encode_msg(#{category_name => CategoryName}, 'PutCategory'),
    encode_raw_bits('PutCategory', Msg).

auth_user(Username, Password) when is_binary(Username) andalso is_binary(Password) ->
    Msg = rubis_pb:encode_msg(#{username => Username, password => Password}, 'AuthUser'),
    encode_raw_bits('AuthUser', Msg).

browse_categories() ->
    Msg = rubis_pb:encode_msg(#{}, 'BrowseCategories'),
    encode_raw_bits('BrowseCategories', Msg).

browse_regions() ->
    Msg = rubis_pb:encode_msg(#{}, 'BrowseRegions'),
    encode_raw_bits('BrowseRegions', Msg).

search_by_category(CategoryId) when is_binary(CategoryId) ->
    Msg = rubis_pb:encode_msg(#{category_id => CategoryId}, 'SearchByCategory'),
    encode_raw_bits('SearchByCategory', Msg).


%% Util functions

%% @doc Encode a server reply as the appropiate proto message
%%
%%      Replies can be either {ok, _} or {error, _}. This encodes
%%      error types as well
-spec enc_resp(atom(), atom(), {ok, any()} | {error, any()}) -> binary().
enc_resp(MsgType, InnerName, {ok, Data}) ->
    rubis_pb:encode_msg(#{resp => {InnerName, Data}}, MsgType);

enc_resp(MsgType, _, {error, Reason}) ->
    rubis_pb:encode_msg(#{resp => {error_reason, encode_error(Reason)}}, MsgType).

%% @doc Decode a server reply proto message to an erlang result
-spec dec_resp(atom(), atom(), binary()) -> {ok, any()} | {error, any()}.
dec_resp(MsgType, InnerName, Msg) ->
    Resp = maps:get(resp, rubis_pb:decode_msg(Msg, MsgType)),
    case Resp of
        {InnerName, Content} ->
            {ok, Content};
        {error_reason, Code} ->
            {error, decode_error(Code)}
    end.

%% @doc Encode Protobuf msg along with msg info
-spec encode_raw_bits(atom(), binary()) -> binary().
encode_raw_bits(Type, Msg) ->
    TypeNum = encode_msg_type(Type),
    <<TypeNum:8, Msg/binary>>.

%% @doc Return msg type and msg from raw bits
-spec decode_raw_bits(binary()) -> {atom(), binary()}.
decode_raw_bits(Bin) ->
    <<N:8, Msg/binary>> = Bin,
    {decode_type_num(N), Msg}.

%% @doc Encode msg type as ints
-spec encode_msg_type(atom()) -> non_neg_integer().
encode_msg_type('PutRegion') -> 1;
encode_msg_type('PutCategory') -> 2;
encode_msg_type('AuthUser') -> 3;
encode_msg_type('RegisterUser') -> 4;
encode_msg_type('BrowseCategories') -> 5;
encode_msg_type('BrowseRegions') -> 6;
encode_msg_type('SearchByCategory') -> 7;
encode_msg_type('SearchByRegion') -> 8;
encode_msg_type('ViewItem') -> 9;
encode_msg_type('ViewUser') -> 10;
encode_msg_type('ViewItemBidHist') -> 11;
encode_msg_type('StoreBuyNow') -> 12;
encode_msg_type('StoreBid') -> 13;
encode_msg_type('StoreComment') -> 14;
encode_msg_type('StoreItem') -> 15;
encode_msg_type('AboutMe') -> 16.

%% @doc Get original message type
-spec decode_type_num(non_neg_integer()) -> atom().
decode_type_num(1) -> 'PutRegion';
decode_type_num(2) -> 'PutCategory';
decode_type_num(3) -> 'AuthUser';
decode_type_num(4) -> 'RegisterUser';
decode_type_num(5) -> 'BrowseCategories';
decode_type_num(6) -> 'BrowseRegions';
decode_type_num(7) -> 'SearchByCategory';
decode_type_num(8) -> 'SearchByRegion';
decode_type_num(9) -> 'ViewItem';
decode_type_num(10) -> 'ViewUser';
decode_type_num(11) -> 'ViewItemBidHist';
decode_type_num(12) -> 'StoreBuyNow';
decode_type_num(13) -> 'StoreBid';
decode_type_num(14) -> 'StoreComment';
decode_type_num(15) -> 'StoreItem';
decode_type_num(16) -> 'AboutMe';
decode_type_num(_) -> unknown.

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
