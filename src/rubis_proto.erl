-module(rubis_proto).

%%
-export([decode_raw_bits/1]).

%% Put Region
-export([put_region_r/1, put_region_d/1,
         put_region_resp_r/1, put_region_resp_d/1]).


%% Put Category
-export([put_category_r/1, put_category_d/1,
         put_category_resp_r/1, put_category_resp_d/1]).

%% Auth User
-export([auth_user_r/2, auth_user_d/1,
         auth_user_resp_r/1, auth_user_resp_d/1]).

%% Browse Categories
-export([browse_categories_r/0, browse_categories_d/1,
         browse_categories_resp_r/1, browse_categories_resp_d/1]).

%% Browse Regions
-export([browse_regions_r/0, browse_regions_d/1,
         browse_regions_resp_r/1, browse_regions_resp_d/1]).

%% Search By Category
-export([search_by_category_r/1, search_by_category_d/1,
         search_by_category_resp_r/1, search_by_category_resp_d/1]).

%% TODO(borja): Implement the rest

put_region_r(RegionName) when is_binary(RegionName) ->
    Msg = rubis:encode_msg(#{region_name => RegionName}, 'PutRegion'),
    encode_raw_bits('PutRegion', Msg).

put_region_d(Msg) ->
    maps:get(region_name, rubis:decode_msg(Msg, 'PutRegion')).

put_region_resp_r(Resp) ->
    enc_resp('PutRegionResp', region_id, Resp).

put_region_resp_d(Msg) ->
    dec_resp('PutRegionResp', region_id, Msg).



put_category_r(CategoryName) when is_binary(CategoryName) ->
    Msg = rubis:encode_msg(#{category_name => CategoryName}, 'PutCategory'),
    encode_raw_bits('PutCategory', Msg).

put_category_d(Msg) ->
    maps:get(category_name, rubis:decode_msg(Msg, 'PutCategory')).

put_category_resp_r(Resp) ->
    enc_resp('PutCategoryResp', category_id, Resp).

put_category_resp_d(Msg) ->
    dec_resp('PutCategoryResp', category_id, Msg).




auth_user_r(Username, Password) when is_binary(Username) andalso is_binary(Password) ->
    Msg = rubis:encode_msg(#{username => Username, password => Password}, 'AuthUser'),
    encode_raw_bits('AuthUser', Msg).

auth_user_d(Msg) ->
    rubis:decode_msg(Msg, 'AuthUser').

auth_user_resp_r(Resp) ->
    enc_resp('AuthUserResp', user_id, Resp).

auth_user_resp_d(Msg) ->
    dec_resp('AuthUserResp', user_id, Msg).



browse_categories_r() ->
    Msg = rubis:encode_msg(#{}, 'BrowseCategories'),
    encode_raw_bits('BrowseCategories', Msg).

browse_categories_d(_) ->
    ok.

browse_categories_resp_r(Resp) ->
    enc_resp('BrowseCategoriesResp', category_names, Resp).

browse_categories_resp_d(Msg) ->
    dec_resp('BrowseCategoriesResp', category_names, Msg).




browse_regions_r() ->
    Msg = rubis:encode_msg(#{}, 'BrowseRegions'),
    encode_raw_bits('BrowseRegions', Msg).

browse_regions_d(_) ->
    ok.

browse_regions_resp_r(Resp) ->
    enc_resp('BrowseRegionsResp', region_names, Resp).

browse_regions_resp_d(Msg) ->
    dec_resp('BrowseRegionsResp', region_names, Msg).




search_by_category_r(CategoryId) when is_binary(CategoryId) ->
    Msg = rubis:encode_msg(#{category_id => CategoryId}, 'SearchByCategory'),
    encode_raw_bits('SearchByCategory', Msg).

search_by_category_d(Msg) ->
    maps:get(category_id, rubis:decode_msg(Msg, 'SearchByCategory')).

search_by_category_resp_r(Resp) ->
    enc_resp('SearchByCategoryResp', items, Resp).

search_by_category_resp_d(Msg) ->
    dec_resp('SearchByCategoryResp', items, Msg).


%% Util functions

enc_resp(MsgType, InnerName, {ok, Data}) ->
    rubis:encode_msg(#{resp => {InnerName, Data}}, MsgType);

enc_resp(MsgType, _, {error, Reason}) ->
    rubis:encode_msg(#{resp => {error_reason, encode_error(Reason)}}, MsgType).

dec_resp(MsgType, InnerName, Msg) ->
    Resp = maps:get(resp, rubis:decode_msg(Msg, MsgType)),
    handle_resp(InnerName, Resp).

-spec handle_resp(atom(), any()) -> {ok, any()} | {error, atom()}.
handle_resp(InnerName, Resp) ->
    case Resp of
        {InnerName, Content} ->
            {ok, Content};
        {error_reason, Code} ->
            {error, decode_error(Code)}
    end.

encode_error(user_not_found) -> 1;
encode_error(wrong_password) -> 2;
encode_error(non_unique_username) -> 3;
encode_error(_) -> 0.


decode_error(0) -> unknown;
decode_error(1) -> user_not_found;
decode_error(2) -> wrong_password;
decode_error(3) -> non_unique_username.

%% @doc Encode Protobuf msg along with msg info
-spec encode_raw_bits(atom(), binary()) -> binary().
encode_raw_bits(Type, Msg) ->
    TypeNum = encode_msg_type(Type),
    <<TypeNum:8, Msg/binary>>.

-spec decode_raw_bits(binary()) -> {atom(), binary()}.
decode_raw_bits(Bin) ->
    <<N:8, Msg/binary>> = Bin,
    {decode_type_num(N), Msg}.

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
