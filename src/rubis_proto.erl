-module(rubis_proto).

-export([peek_msg_type/1]).

%% Generic client - server side methods
-export([decode_request/1,
         encode_reply/2,
         decode_reply/2]).

%% Init DB
-export([put_region/1,
         put_category/1]).

%% Read-write RUBIS Procedures
-export([auth_user/2,
         register_user/3,
         store_buy_now/3,
         store_bid/3,
         store_comment/5,
         store_item/5]).

%% Read-only RUBIS Procedures
-export([browse_categories/0,
         browse_regions/0,
         search_items_by_category/1,
         search_items_by_region/2,
         view_item/1,
         view_user/1,
         view_bid_history/1,
         about_me/1]).

-spec peek_msg_type(binary()) -> atom().
peek_msg_type(Bin) ->
    {Type, _} = decode_raw_bits(Bin),
    Type.

%% @doc Generic server side decode
-spec decode_request(binary()) -> {atom(), #{}}.
decode_request(Bin) ->
    {Type, BinMsg} = decode_raw_bits(Bin),
    {Type, rubis_pb:decode_msg(BinMsg, Type)}.

%% @doc Generic client side decode
%%
%%      First argument is the original request
decode_reply('PutRegion', Msg) ->
    dec_resp('PutRegionResp', region_id, Msg);

decode_reply('PutCategory', Msg) ->
    dec_resp('PutCategoryResp', category_id, Msg);

decode_reply('AuthUser', Msg) ->
    dec_resp('AuthUserResp', user_id, Msg);

decode_reply('RegisterUser', Msg) ->
    dec_resp('RegisterUserResp', user_id, Msg);

decode_reply('BrowseCategories', Msg) ->
    dec_resp('BrowseCategoriesResp', success, Msg);

decode_reply('BrowseRegions', Msg) ->
    dec_resp('BrowseRegionsResp', success, Msg);

decode_reply('SearchByCategory', Msg) ->
    dec_resp('SearchByCategoryResp', success, Msg);

decode_reply('SearchByRegion', Msg) ->
    dec_resp('SearchByRegionResp', success, Msg);

decode_reply('ViewItem', Msg) ->
    dec_resp('ViewItemResp', success, Msg);

decode_reply('ViewUser', Msg) ->
    dec_resp('ViewUserResp', success, Msg);

decode_reply('ViewItemBidHist', Msg) ->
    dec_resp('ViewItemBidHistResp', success, Msg);

decode_reply('StoreBuyNow', Resp) ->
    dec_resp('StoreBuyNowResp', buy_now_id, Resp);

decode_reply('StoreBid', Msg) ->
    dec_resp('StoreBidResp', bid_id, Msg);

decode_reply('StoreComment', Resp) ->
    dec_resp('StoreCommentResp', comment_id, Resp);

decode_reply('StoreItem', Msg) ->
    dec_resp('StoreItemResp', item_id, Msg);

decode_reply('AboutMe', Msg) ->
    dec_resp('AboutMeResp', success, Msg).

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

encode_reply('RegisterUser', Resp) ->
    enc_resp('RegisterUserResp', user_id, Resp);

encode_reply('BrowseCategories', Resp) ->
    enc_resp('BrowseCategoriesResp', success, Resp);

encode_reply('BrowseRegions', Resp) ->
    enc_resp('BrowseRegionsResp', success, Resp);

encode_reply('SearchByCategory', Resp) ->
    enc_resp('SearchByCategoryResp', success, Resp);

encode_reply('SearchByRegion', Resp) ->
    enc_resp('SearchByRegionResp', success, Resp);

encode_reply('ViewItem', Resp) ->
    enc_resp('ViewItemResp', success, Resp);

encode_reply('ViewUser', Resp) ->
    enc_resp('ViewUserResp', success, Resp);

encode_reply('ViewItemBidHist', Resp) ->
    enc_resp('ViewItemBidHistResp', success, Resp);

encode_reply('StoreBuyNow', Resp) ->
    enc_resp('StoreBuyNowResp', buy_now_id, Resp);

encode_reply('StoreBid', Resp) ->
    enc_resp('StoreBidResp', bid_id, Resp);

encode_reply('StoreComment', Resp) ->
    enc_resp('StoreCommentResp', comment_id, Resp);

encode_reply('StoreItem', Resp) ->
    enc_resp('StoreItemResp', item_id, Resp);

encode_reply('AboutMe', Resp) ->
    enc_resp('AboutMeResp', success, Resp).

put_region(RegionName) when is_binary(RegionName) ->
    Msg = rubis_pb:encode_msg(#{region_name => RegionName}, 'PutRegion'),
    encode_raw_bits('PutRegion', Msg).

put_category(CategoryName) when is_binary(CategoryName) ->
    Msg = rubis_pb:encode_msg(#{category_name => CategoryName}, 'PutCategory'),
    encode_raw_bits('PutCategory', Msg).

auth_user(Username, Password) when is_binary(Username) andalso is_binary(Password) ->
    Msg = rubis_pb:encode_msg(#{username => Username, password => Password}, 'AuthUser'),
    encode_raw_bits('AuthUser', Msg).

register_user(Username, Password, RegionId) ->
    Msg = rubis_pb:encode_msg(#{username => Username,
                                password => Password,
                                region_id => RegionId}, 'RegisterUser'),
    encode_raw_bits('RegisterUser', Msg).

browse_categories() ->
    Msg = rubis_pb:encode_msg(#{}, 'BrowseCategories'),
    encode_raw_bits('BrowseCategories', Msg).

browse_regions() ->
    Msg = rubis_pb:encode_msg(#{}, 'BrowseRegions'),
    encode_raw_bits('BrowseRegions', Msg).

search_items_by_category(CategoryId) when is_binary(CategoryId) ->
    Msg = rubis_pb:encode_msg(#{category_id => CategoryId}, 'SearchByCategory'),
    encode_raw_bits('SearchByCategory', Msg).

search_items_by_region(CategoryId, RegionId) when is_binary(RegionId) ->
    Msg = rubis_pb:encode_msg(#{category_id => CategoryId,
                                region_id => RegionId}, 'SearchByRegion'),
    encode_raw_bits('SearchByRegion', Msg).

view_item(ItemId) when is_binary(ItemId) ->
    Msg = rubis_pb:encode_msg(#{item_id => ItemId}, 'ViewItem'),
    encode_raw_bits('ViewItem', Msg).

view_user(UserId) when is_binary(UserId) ->
    Msg = rubis_pb:encode_msg(#{user_id => UserId}, 'ViewUser'),
    encode_raw_bits('ViewUser', Msg).

view_bid_history(ItemId) when is_binary(ItemId) ->
    Msg = rubis_pb:encode_msg(#{item_id => ItemId}, 'ViewItemBidHist'),
    encode_raw_bits('ViewItemBidHist', Msg).

about_me(UserId) when is_binary(UserId) ->
    Msg = rubis_pb:encode_msg(#{user_id => UserId}, 'AboutMe'),
    encode_raw_bits('AboutMe', Msg).

store_buy_now(ItemId, BuyerId, Value) ->
    Msg = rubis_pb:encode_msg(#{on_item_id => ItemId,
                                buyer_id => BuyerId,
                                quantity => Value}, 'StoreBuyNow'),
    encode_raw_bits('StoreBuyNow', Msg).

store_bid(ItemId, BidderId, Value) ->
    Msg = rubis_pb:encode_msg(#{on_item_id => ItemId,
                                bidder_id => BidderId,
                                value => Value}, 'StoreBid'),
    encode_raw_bits('StoreBid', Msg).

store_comment(ItemId, FromId, ToId, Rating, Body) ->
    Msg = rubis_pb:encode_msg(#{on_item_id => ItemId,
                                from_id => FromId,
                                to_id => ToId,
                                rating => Rating,
                                body => Body}, 'StoreComment'),
    encode_raw_bits('StoreComment', Msg).

store_item(ItemName, ItemDesc, Quantity, CategoryId, SellerId) ->
    Msg = rubis_pb:encode_msg(#{item_name => ItemName,
                                description => ItemDesc,
                                quantity => Quantity,
                                category_id => CategoryId,
                                seller_id => SellerId}, 'StoreItem'),
    encode_raw_bits('StoreItem', Msg).


%% Util functions

%% @doc Encode a server reply as the appropiate proto message
%%
%%      Replies can be either {ok, _} or {error, _}. This encodes
%%      error types as well
-spec enc_resp(atom(), atom(), ok | {ok, any()} | {error, any()}) -> binary().
enc_resp(MsgType, success, ok) ->
    rubis_pb:encode_msg(#{resp => {success, encode_success(ok)}}, MsgType);

enc_resp(MsgType, InnerName, {ok, Data}) ->
    rubis_pb:encode_msg(#{resp => {InnerName, Data}}, MsgType);

enc_resp(MsgType, _, {error, Reason}) ->
    rubis_pb:encode_msg(#{resp => {error_reason, encode_error(Reason)}}, MsgType).

%% @doc Decode a server reply proto message to an erlang result
-spec dec_resp(atom(), atom(), binary()) -> {ok, any()} | {error, any()}.
dec_resp(MsgType, InnerName, Msg) ->
    Resp = maps:get(resp, rubis_pb:decode_msg(Msg, MsgType)),
    case Resp of
        {success, Code} ->
            decode_success(Code);

        {error_reason, Code} ->
            {error, decode_error(Code)};

        {InnerName, Content} ->
            {ok, Content}
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

-spec encode_success(atom()) -> non_neg_integer().
-spec decode_success(non_neg_integer()) -> atom().
encode_success(_) -> 1.
decode_success(_) -> ok.

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
