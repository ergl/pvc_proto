-module(ppb_rubis_driver).

-export([from_client_dec/1,
         to_client_enc/2,
         from_server_dec/1]).

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

-spec from_client_dec(binary()) -> {atom(), #{}}.
from_client_dec(Bin) ->
    {Type, BinMsg} = decode_raw_bits(Bin),
    {Type, rubis_msgs:decode_msg(BinMsg, Type)}.

%% @doc Generic client side decode
from_server_dec(Bin) ->
    {Type, BinMsg} = decode_raw_bits(Bin),
    dec_resp(Type, reply_field_name(Type), BinMsg).

%% @doc Generic client side encode
%%
%%      First argument is the original request
%%      Replies don't need to be wrapped in message identifiers
to_client_enc('PutRegion', Resp) ->
    enc_resp('PutRegionResp', region_id, Resp);

to_client_enc('PutCategory', Resp) ->
    enc_resp('PutCategoryResp', category_id, Resp);

to_client_enc('AuthUser', Resp) ->
    enc_resp('AuthUserResp', user_id, Resp);

to_client_enc('RegisterUser', Resp) ->
    enc_resp('RegisterUserResp', user_id, Resp);

to_client_enc('BrowseCategories', Resp) ->
    enc_resp('BrowseCategoriesResp', success, Resp);

to_client_enc('BrowseRegions', Resp) ->
    enc_resp('BrowseRegionsResp', success, Resp);

to_client_enc('SearchByCategory', Resp) ->
    enc_resp('SearchByCategoryResp', success, Resp);

to_client_enc('SearchByRegion', Resp) ->
    enc_resp('SearchByRegionResp', success, Resp);

to_client_enc('ViewItem', Resp) ->
    enc_resp('ViewItemResp', success, Resp);

to_client_enc('ViewUser', Resp) ->
    enc_resp('ViewUserResp', success, Resp);

to_client_enc('ViewItemBidHist', Resp) ->
    enc_resp('ViewItemBidHistResp', success, Resp);

to_client_enc('StoreBuyNow', Resp) ->
    enc_resp('StoreBuyNowResp', buy_now_id, Resp);

to_client_enc('StoreBid', Resp) ->
    enc_resp('StoreBidResp', bid_id, Resp);

to_client_enc('StoreComment', Resp) ->
    enc_resp('StoreCommentResp', comment_id, Resp);

to_client_enc('StoreItem', Resp) ->
    enc_resp('StoreItemResp', item_id, Resp);

to_client_enc('AboutMe', Resp) ->
    enc_resp('AboutMeResp', success, Resp).

put_region(RegionName) when is_binary(RegionName) ->
    Msg = rubis_msgs:encode_msg(#{region_name => RegionName}, 'PutRegion'),
    encode_raw_bits('PutRegion', Msg).

put_category(CategoryName) when is_binary(CategoryName) ->
    Msg = rubis_msgs:encode_msg(#{category_name => CategoryName}, 'PutCategory'),
    encode_raw_bits('PutCategory', Msg).

auth_user(Username, Password) when is_binary(Username) andalso is_binary(Password) ->
    Msg = rubis_msgs:encode_msg(#{username => Username, password => Password}, 'AuthUser'),
    encode_raw_bits('AuthUser', Msg).

register_user(Username, Password, RegionId) ->
    Msg = rubis_msgs:encode_msg(#{username => Username,
                                password => Password,
                                region_id => RegionId}, 'RegisterUser'),
    encode_raw_bits('RegisterUser', Msg).

browse_categories() ->
    Msg = rubis_msgs:encode_msg(#{}, 'BrowseCategories'),
    encode_raw_bits('BrowseCategories', Msg).

browse_regions() ->
    Msg = rubis_msgs:encode_msg(#{}, 'BrowseRegions'),
    encode_raw_bits('BrowseRegions', Msg).

search_items_by_category(CategoryId) when is_binary(CategoryId) ->
    Msg = rubis_msgs:encode_msg(#{category_id => CategoryId}, 'SearchByCategory'),
    encode_raw_bits('SearchByCategory', Msg).

search_items_by_region(CategoryId, RegionId) when is_binary(RegionId) ->
    Msg = rubis_msgs:encode_msg(#{category_id => CategoryId,
                                region_id => RegionId}, 'SearchByRegion'),
    encode_raw_bits('SearchByRegion', Msg).

view_item(ItemId) when is_binary(ItemId) ->
    Msg = rubis_msgs:encode_msg(#{item_id => ItemId}, 'ViewItem'),
    encode_raw_bits('ViewItem', Msg).

view_user(UserId) when is_binary(UserId) ->
    Msg = rubis_msgs:encode_msg(#{user_id => UserId}, 'ViewUser'),
    encode_raw_bits('ViewUser', Msg).

view_bid_history(ItemId) when is_binary(ItemId) ->
    Msg = rubis_msgs:encode_msg(#{item_id => ItemId}, 'ViewItemBidHist'),
    encode_raw_bits('ViewItemBidHist', Msg).

about_me(UserId) when is_binary(UserId) ->
    Msg = rubis_msgs:encode_msg(#{user_id => UserId}, 'AboutMe'),
    encode_raw_bits('AboutMe', Msg).

store_buy_now(ItemId, BuyerId, Value) ->
    Msg = rubis_msgs:encode_msg(#{on_item_id => ItemId,
                                buyer_id => BuyerId,
                                quantity => Value}, 'StoreBuyNow'),
    encode_raw_bits('StoreBuyNow', Msg).

store_bid(ItemId, BidderId, Value) ->
    Msg = rubis_msgs:encode_msg(#{on_item_id => ItemId,
                                bidder_id => BidderId,
                                value => Value}, 'StoreBid'),
    encode_raw_bits('StoreBid', Msg).

store_comment(ItemId, FromId, ToId, Rating, Body) ->
    Msg = rubis_msgs:encode_msg(#{on_item_id => ItemId,
                                from_id => FromId,
                                to_id => ToId,
                                rating => Rating,
                                body => Body}, 'StoreComment'),
    encode_raw_bits('StoreComment', Msg).

store_item(ItemName, ItemDesc, Quantity, CategoryId, SellerId) ->
    Msg = rubis_msgs:encode_msg(#{item_name => ItemName,
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
    enc_resp_int(
        MsgType,
        rubis_msgs:encode_msg(
            #{resp => {success, common:encode_success(ok)}},
            MsgType
        )
    );

enc_resp(MsgType, InnerName, {ok, Data}) ->
    enc_resp_int(
        MsgType,
        rubis_msgs:encode_msg(
            #{resp => {InnerName, Data}},
            MsgType
        )
    );

enc_resp(MsgType, _, {error, Reason}) ->
    enc_resp_int(
        MsgType,
        rubis_msgs:encode_msg(
            #{resp => {error_reason, common:encode_error(Reason)}},
            MsgType
        )
    ).

enc_resp_int(MsgType, Msg) ->
    encode_raw_bits(MsgType, Msg).

%% @doc Decode a server reply proto message to an erlang result
-spec dec_resp(atom(), atom(), binary()) -> {ok, any()} | {error, any()}.
dec_resp(MsgType, InnerName, Msg) ->
    Resp = maps:get(resp, rubis_msgs:decode_msg(Msg, MsgType)),
    case Resp of
        {success, Code} ->
            common:decode_success(Code);

        {error_reason, Code} ->
            {error, common:decode_error(Code)};

        {InnerName, Content} ->
            {ok, Content}
    end.

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

%% @doc Get inner field from message type
reply_field_name('PutRegionResp') -> region_id;
reply_field_name('PutCategoryResp') -> category_id;
reply_field_name('AuthUserResp') -> user_id;
reply_field_name('RegisterUserResp') -> user_id;
reply_field_name('BrowseCategoriesResp') -> success;
reply_field_name('BrowseRegionsResp') -> success;
reply_field_name('SearchByCategoryResp') -> success;
reply_field_name('SearchByRegionResp') -> success;
reply_field_name('ViewItemResp') -> success;
reply_field_name('ViewUserResp') -> success;
reply_field_name('ViewItemBidHistResp') -> success;
reply_field_name('StoreBuyNowResp') -> buy_now_id;
reply_field_name('StoreBidResp') -> bid_id;
reply_field_name('StoreCommentResp') -> comment_id;
reply_field_name('StoreItemResp') -> item_id;
reply_field_name('AboutMeResp') -> success.

%% @doc Encode msg type as ints

%% Client Requests
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
encode_msg_type('AboutMe') -> 16;

%% Server Responses
encode_msg_type('PutRegionResp') -> 17;
encode_msg_type('PutCategoryResp') -> 18;
encode_msg_type('AuthUserResp') -> 19;
encode_msg_type('RegisterUserResp') -> 20;
encode_msg_type('BrowseCategoriesResp') -> 21;
encode_msg_type('BrowseRegionsResp') -> 22;
encode_msg_type('SearchByCategoryResp') -> 23;
encode_msg_type('SearchByRegionResp') -> 24;
encode_msg_type('ViewItemResp') -> 25;
encode_msg_type('ViewUserResp') -> 26;
encode_msg_type('ViewItemBidHistResp') -> 27;
encode_msg_type('StoreBuyNowResp') -> 28;
encode_msg_type('StoreBidResp') -> 29;
encode_msg_type('StoreCommentResp') -> 30;
encode_msg_type('StoreItemResp') -> 31;
encode_msg_type('AboutMeResp') -> 32.

%% @doc Get original message type
-spec decode_type_num(non_neg_integer()) -> atom().

%% Client Requests
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

%% Server Responses
decode_type_num(17) -> 'PutRegionResp';
decode_type_num(18) -> 'PutCategoryResp';
decode_type_num(19) -> 'AuthUserResp';
decode_type_num(20) -> 'RegisterUserResp';
decode_type_num(21) -> 'BrowseCategoriesResp';
decode_type_num(22) -> 'BrowseRegionsResp';
decode_type_num(23) -> 'SearchByCategoryResp';
decode_type_num(24) -> 'SearchByRegionResp';
decode_type_num(25) -> 'ViewItemResp';
decode_type_num(26) -> 'ViewUserResp';
decode_type_num(27) -> 'ViewItemBidHistResp';
decode_type_num(28) -> 'StoreBuyNowResp';
decode_type_num(29) -> 'StoreBidResp';
decode_type_num(30) -> 'StoreCommentResp';
decode_type_num(31) -> 'StoreItemResp';
decode_type_num(32) -> 'AboutMeResp'.