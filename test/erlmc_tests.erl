-module(erlmc_tests).
-include_lib("eunit/include/eunit.hrl").
-define(setup(F), {setup, fun start/0, fun stop/1, F}).
 
%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% TESTS DESCRIPTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%
start_stop_test_() ->
    {"The driver can be started, stopped and check availability of memcached at localhost:11211",
     ?setup(fun is_app_started/1)}.
memcached_ops_test_() ->
    {"Test main Memcached protocol operations",
     ?setup(fun mc_ops/1)}.
%%%%%%%%%%%%%%%%%%%%%%%
%%% SETUP FUNCTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%
start() ->
    McIP="localhost",
    McPort=11211,
    application:set_env(erlotpmc,mcservers,[{McIP, McPort,1}]),
    ok=application:start(erlotpmc).
stop(_) ->
    application:stop(erlotpmc).
%%%%%%%%%%%%%%%%%%%%
%%% ACTUAL TESTS %%%
%%%%%%%%%%%%%%%%%%%%
is_app_started(_) -> 
    [?_assert(lists:keymember(erlotpmc,1,application:which_applications()))].

mc_ops(_) ->
    [
     ?_assertEqual( <<>>, erlmc:set("Hello", <<"World">>)),
     ?_assertEqual( <<"Data exists for key.">>, erlmc:add("Hello", <<"Fail">>)),
     ?_assertEqual( <<"World">>,erlmc:get("Hello")),
     ?_assertEqual( <<>>, erlmc:delete("Hello")),
     ?_assertEqual( <<>>, erlmc:add("Hello", <<"World2">>)),
     ?_assertEqual( <<"World2">>,erlmc:get("Hello")),
     ?_assertEqual( <<>>, erlmc:append("Hello", <<"!!!">>)),
     ?_assertEqual( <<"World2!!!">>, erlmc:get("Hello")),
     ?_assertEqual( <<>>, erlmc:prepend("Hello", <<"$$$">>)),
     ?_assertEqual( <<"$$$World2!!!">>, erlmc:get("Hello")),
     ?_assertEqual( <<>>, erlmc:delete("Hello")),     
     ?_assertEqual( <<>>, erlmc:get("Hello")),     
     ?_assertEqual( <<>>, erlmc:set("One", <<"A">>)),     
     ?_assertEqual( <<>>, erlmc:set("Two", <<"B">>)),
     ?_assertEqual( <<>>, erlmc:set("Three", <<"C">>)),
     ?_assertEqual([{"One",<<"A">>},{"Two",<<"B">>},{"Two-and-a-half",<<>>},{"Three",<<"C">>}], erlmc:get_many(["One", "Two", "Two-and-a-half", "Three"])),
     ?_assertEqual([{{"localhost",11211},<<>>}], erlmc:flush(0)),
     ?_assertMatch([{{"localhost",11211}, [{_,_}|_]}],erlmc:stats()),
     ?_assertMatch([{_,_}|_],erlmc:stats("localhost",11211)),
     ?_assertMatch([{{"localhost",11211},[true]}],erlmc:quit()),
     ?_assertEqual( {has_server_result,true}, erlmc:has_server("localhost",11211)),
     ?_assertEqual( ok, erlmc:remove_server("localhost",11211)),
     ?_assertEqual( {has_server_result,false}, erlmc:has_server("localhost",11211))
    ].
%%%%%%%%%%%%%%%%%%%%%%%%
%%% HELPER FUNCTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%
