-module(rubis_proto).

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

%% TODO(borja): Implement the rest

put_region_r(RegionName) when is_binary(RegionName) ->
    rubis:encode_msg(#{region_name => RegionName}, 'PutRegion').

put_region_d(Msg) ->
    maps:get(region_name, rubis:decode_msg(Msg, 'PutRegion')).

put_region_resp_r(RegionId) when is_binary(RegionId) ->
    rubis:encode_msg(#{region_id => RegionId}, 'PutRegionResp').

put_region_resp_d(Msg) ->
    maps:get(region_id, rubis:decode_msg(Msg, 'PutRegionResp')).




put_category_r(CategoryName) when is_binary(CategoryName) ->
    rubis:encode_msg(#{category_name => CategoryName}, 'PutCategory').

put_category_d(Msg) ->
    maps:get(category_name, rubis:decode_msg(Msg, 'PutCategory')).

put_category_resp_r(CategoryId) when is_binary(CategoryId) ->
    rubis:encode_msg(#{category_id => CategoryId}, 'PutCategoryResp').

put_category_resp_d(Msg) ->
    maps:get(category_id, rubis:decode_msg(Msg, 'PutCategoryResp')).




auth_user_r(Username, Password) when is_binary(Username) andalso is_binary(Password) ->
    rubis:encode_msg(#{username => Username, password => Password}, 'AuthUser').

auth_user_d(Msg) ->
    rubis:decode_msg(Msg, 'AuthUser').

auth_user_resp_r({ok, UserId}) when is_binary(UserId) ->
    rubis:encode_msg(#{resp => {user_id, UserId}}, 'AuthUserResp');

auth_user_resp_r({error, Reason}) ->
    rubis:encode_msg(#{resp => {error_reason, encode_error(Reason)}}, 'AuthUserResp').

auth_user_resp_d(Msg) ->
    Resp = maps:get(resp, rubis:decode_msg(Msg, 'AuthUserResp')),
    case Resp of
        {user_id, Id} ->
            {ok, Id};
        {error_reason, Code} ->
            {error, decode_error(Code)}
    end.




browse_categories_r() ->
    rubis:encode_msg(#{}, 'BrowseCategories').

browse_categories_d(_) ->
    ok.

browse_categories_resp_r(Names) when is_list(Names) ->
    rubis:encode_msg(#{category_names => Names}, 'BrowseCategoriesResp');

browse_categories_resp_r(Name) ->
    browse_categories_resp_r([Name]).

browse_categories_resp_d(Msg) ->
    maps:get(category_names, rubis:decode_msg(Msg, 'BrowseCategoriesResp')).




browse_regions_r() ->
    rubis:encode_msg(#{}, 'BrowseRegions').

browse_regions_d(_) ->
    ok.

browse_regions_resp_r(Names) when is_list(Names) ->
    rubis:encode_msg(#{region_names => Names}, 'BrowseRegionsResp');

browse_regions_resp_r(Name) ->
    browse_regions_resp_r([Name]).

browse_regions_resp_d(Msg) ->
    maps:get(region_names, rubis:decode_msg(Msg, 'BrowseRegionsResp')).


%% Util functions

encode_error(user_not_found) -> 1;
encode_error(wrong_password) -> 2;
encode_error(non_unique_username) -> 3;
encode_error(_) -> 0.


decode_error(0) -> unknown;
decode_error(1) -> user_not_found;
decode_error(2) -> wrong_password;
decode_error(3) -> non_unique_username.