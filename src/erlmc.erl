%%%-------------------------------------------------------------------
%%% @author Alexey Larin <>
%%% @copyright (C) 2012, Alexey Larin
%%% @doc
%%%
%%% @end
%%% Created : 30 Mar 2012 by Alexey Larin <>
%%%-------------------------------------------------------------------
%%
%% http://code.google.com/p/memcached/wiki/MemcacheBinaryProtocol
%% @doc a binary protocol memcached client
-module(erlmc).

-behaviour(gen_server).

-include("erlmc.hrl").

%% API
-export([start_link/0,start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE). 

-define(TIMEOUT, 60000).

-define(CMONTIME, 5000).

%-define(DEBUG(Format, Args),io:format("~s.~w: DEBUG: " ++ Format, [ ?MODULE, ?LINE | Args])).
-define(DEBUG(Format, Args), true).

-record(state, {
	  cacheservers = sets:new(),
	  cmon_time_ref
	  }).

%%%===================================================================
%%% API
%%%===================================================================

-export([add_server/3, remove_server/2, refresh_server/3, has_server/2,
         add_connection/2, remove_connection/2,refresh_all_servers/0]).

%% api callbacks
-export([get/1, get_many/1, add/2, add/3, set/2, set/3, 
                 replace/2, replace/3, delete/1, increment/4, decrement/4,
                 append/2, prepend/2, stats/0, stats/2, flush/0, flush/1, quit/0, 
                 version/0]).
%%--------------------------------------------------------------------
%%% API
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    Servers = case application:get_env(erlotpmc,mcservers) of
        {ok, CacheServers} -> CacheServers;
        undefined -> []
    end,
    start_link(Servers).

start_link(CacheServers) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [CacheServers], []).

add_server(Host, Port, PoolSize) ->
    gen_server:cast(?MODULE, {add_server, Host, Port, PoolSize}).

refresh_server(Host, Port, PoolSize) ->
    gen_server:cast(?MODULE, {refresh_server, Host, Port, PoolSize}).

refresh_all_servers() ->
    gen_server:cast(?MODULE, refresh_all_servers).

remove_server(Host, Port) ->
    gen_server:cast(?MODULE, {remove_server, Host, Port}).

has_server(Host, Port) ->
    gen_server:call(?MODULE, {has_server, self(), Host, Port}).

add_connection(Host, Port) ->
    gen_server:cast(?MODULE, {add_connection, Host, Port}).

remove_connection(Host, Port) ->
    gen_server:cast(?MODULE, {remove_connection, Host, Port}).

get(Key0) ->
    Key = package_key(Key0),
    call(map_key(Key), {get, Key}, ?TIMEOUT).

get_many(Keys) ->
    Self = self(),
    Pids = [spawn(fun() -> 
                          Res = (catch ?MODULE:get(Key)),
                          Self ! {self(), {Key, Res}}
                  end) || Key <- Keys],
    lists:reverse(lists:foldl(
                    fun(Pid, Acc) ->
                            receive
                                {Pid, {Key, Res}} -> [{Key, Res}|Acc]
                            after ?TIMEOUT ->
                                    Acc
                            end
                    end, [], Pids)).

add(Key, Value) ->
    add(Key, Value, 0).

add(Key0, Value, Expiration) when is_binary(Value), is_integer(Expiration) ->
    Key = package_key(Key0),
    call(map_key(Key), {add, Key, Value, Expiration}, ?TIMEOUT).

set(Key, Value) ->
    set(Key, Value, 0).

set(Key0, Value, Expiration) when is_binary(Value), is_integer(Expiration) ->
    Key = package_key(Key0),
    call(map_key(Key), {set, Key, Value, Expiration}, ?TIMEOUT).

replace(Key, Value) ->
    replace(Key, Value, 0).

replace(Key0, Value, Expiration) when is_binary(Value), is_integer(Expiration) ->
    Key = package_key(Key0),
    call(map_key(Key), {replace, Key, Value, Expiration}, ?TIMEOUT).

delete(Key0) ->
    Key = package_key(Key0),
    call(map_key(Key), {delete, Key}, ?TIMEOUT).

increment(Key0, Value, Initial, Expiration) when is_binary(Value), is_binary(Initial), is_integer(Expiration) ->
    Key = package_key(Key0),
    call(map_key(Key), {increment, Key, Value, Initial, Expiration}, ?TIMEOUT).

decrement(Key0, Value, Initial, Expiration) when is_binary(Value), is_binary(Initial), is_integer(Expiration) ->
    Key = package_key(Key0),
    call(map_key(Key), {decrement, Key, Value, Initial, Expiration}, ?TIMEOUT).

append(Key0, Value) when is_binary(Value) ->
    Key = package_key(Key0),
    call(map_key(Key), {append, Key, Value}, ?TIMEOUT).

prepend(Key0, Value) when is_binary(Value) ->
    Key = package_key(Key0),
    call(map_key(Key), {prepend, Key, Value}, ?TIMEOUT).

stats() ->
    multi_call(stats).

stats(Host, Port) ->
    host_port_call(Host, Port, stats).

flush() ->
    multi_call(flush).

flush(Expiration) when is_integer(Expiration) ->
    multi_call({flush, Expiration}).

quit() ->
    [begin
         {Key, [
                {'EXIT',{shutdown,{gen_server,call,[Pid,quit,?TIMEOUT]}}} == 
                    (catch gen_server:call(Pid, quit, ?TIMEOUT)) || Pid <- Pids]}
     end || {Key, Pids} <- unique_connections()].

version() ->
    multi_call(version).

multi_call(Msg) ->
    [begin
         Pid = lists:nth(random:uniform(length(Pids)), Pids),
         {{Host, Port}, gen_server:call(Pid, Msg, ?TIMEOUT)}
     end || {{Host, Port}, Pids} <- unique_connections()].

host_port_call(Host, Port, Msg) ->
    Pid = unique_connection(Host, Port),
    gen_server:call(Pid, Msg, ?TIMEOUT).

call(Pid, Msg, Timeout) ->
    case gen_server:call(Pid, Msg, Timeout) of
        {error, Error} -> exit({erlmc, Error});
        Resp -> Resp
    end.
        
%%--------------------------------------------------------------------
%%% Stateful loop
%%--------------------------------------------------------------------  
init([CacheServers]) ->
    ets:new(erlmc_continuum, [ordered_set, named_table, {read_concurrency, true}]),
    ets:new(erlmc_connections, [bag, named_table]),

    %% Continuum = [{uint(), {Host, Port}}]
    [add_server_to_continuum(Host, Port) || {Host, Port, _} <- CacheServers],

    %% Connections = [{{Host,Port}, ConnPid}]
    [begin
         [start_connection(Host, Port) || _ <- lists:seq(1, ConnPoolSize)]
     end || {Host, Port, ConnPoolSize} <- CacheServers],
    WDT=case application:get_env(erlotpmc,wd_timer) of
            {ok,Val} when is_integer(Val) -> Val;
            _Else -> ?CMONTIME
        end,
    Cmon_timer_ref=erlang:send_after(WDT,self(),{cmontime,WDT}),
    {ok, #state{cacheservers=sets:from_list(CacheServers),
		cmon_time_ref=Cmon_timer_ref}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({has_server, _CallerPid, Host, Port}, _From, State) ->
    Reply = {has_server_result, is_server_in_continuum(Host, Port)},
    {reply, Reply, State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.
        
%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast({add_server, Host, Port, ConnPoolSize},#state{cacheservers=CS}=State) ->
    add_server_to_continuum(Host, Port),
    [start_connection(Host, Port) || _ <- lists:seq(1, ConnPoolSize)],
    CS2=sets:add_element({Host, Port, ConnPoolSize},CS),
    {noreply, State#state{cacheservers=CS2}};

handle_cast({refresh_server, Host, Port, ConnPoolSize}, State) ->
    %% adding to continuum is idempotent
    add_server_to_continuum(Host, Port),
    %% add only necessary connections to reach pool size
    LiveConnections = revalidate_connections(Host, Port),
    if
        LiveConnections < ConnPoolSize ->
            error_logger:warning_msg("Refill pool connections for server: ~p~n ",[{Host, Port}]),
            [start_connection(Host, Port) || _ <- lists:seq(1, ConnPoolSize - LiveConnections)];
        true ->
            ok
    end,
    {noreply, State};
handle_cast({remove_server, Host, Port},#state{cacheservers=CS}=State) ->
    [(catch gen_server:call(Pid, quit, ?TIMEOUT)) || [Pid] <- ets:match(erlmc_connections, {{Host, Port}, '$1'})],
    remove_server_from_continuum(Host, Port),
    L1=[{H,P,PL}||{H,P,PL} <- sets:to_list(CS), H == Host, P==Port],
    CS2=sets:subtract(CS,sets:from_list(L1)),
    {noreply, State#state{cacheservers=CS2}};

handle_cast({add_connection, Host, Port},State) ->
    start_connection(Host, Port),
    {noreply, State};

handle_cast({remove_connection, Host, Port},State) ->
    [[Pid]|_] = ets:match(erlmc_connections, {{Host, Port}, '$1'}),
    (catch gen_server:call(Pid, quit, ?TIMEOUT)),
    {noreply, State};

handle_cast(refresh_all_servers,#state{cacheservers=CS}=State) ->
    ?DEBUG("Refresh all servers ~p~n:",[CacheServers]),
    [refresh_server(Host, Port,PoolSize)||{Host,Port,PoolSize} <- sets:to_list(CS)],
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info({'DOWN',_MonitorRef,process,Pid, Err},State) ->
    ?DEBUG("Recived DOWN msg: ~p",[{'DOWN',_MonitorRef,process,Pid, Err}]),
    error_logger:error_msg("Recived DOWN msg: ~p",[{'DOWN',_MonitorRef,process,Pid, Err}]),
         case ets:match(erlmc_connections, {'$1', Pid}) of
        [[{Host, Port}]] -> 
            ets:delete_object(erlmc_connections, {{Host, Port}, Pid}),
            case Err of
                shutdown -> ok;
                _ -> start_connection(Host, Port)
            end;
        _ -> 
            ok
    end,
    {noreply, State};
handle_info({cmontime,NextTime},State) ->
    refresh_all_servers(),
    Cmon_timer_ref=erlang:send_after(NextTime,self(),{cmontime,NextTime}),
    ?DEBUG("Recived cmontime with NextTime=~p, Recreate TimerRef=~p~n",[NextTime,Cmon_timer_ref]),
    {noreply, State#state{cmon_time_ref=Cmon_timer_ref}};

handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
        
start_connection(Host, Port) ->
        case erlmc_conn:start([Host, Port]) of
                {ok, Pid} -> ets:insert(erlmc_connections, {{Host, Port}, Pid}),
                             erlang:monitor(process,Pid);
                _ -> ok
        end.

revalidate_connections(Host, Port) ->
    [(catch gen_server:call(Pid, version, ?TIMEOUT)) || [Pid] <- ets:match(erlmc_connections, {{Host, Port}, '$1'})],
    length(ets:match(erlmc_connections, {{Host, Port}, '$1'})).

add_server_to_continuum(Host, Port) ->
        [ets:insert(erlmc_continuum, {hash_to_uint(Host ++ integer_to_list(Port) ++ integer_to_list(I)), {Host, Port}}) || I <- lists:seq(1, 100)].

remove_server_from_continuum(Host, Port) ->
        case ets:match(erlmc_continuum, {'$1', {Host, Port}}) of
                [] -> 
                        ok;
                List ->
                        [ets:delete(erlmc_continuum, Key) || [Key] <- List]
        end.

is_server_in_continuum(Host, Port) ->
    case ets:match(erlmc_continuum, {'$1', {Host, Port}}) of
        [] -> 
            false;
        _ ->
            true
    end.

package_key(Key) when is_atom(Key) ->
    atom_to_list(Key);

package_key(Key) when is_list(Key) ->
    Key;

package_key(Key) when is_binary(Key) ->
    binary_to_list(Key);

package_key(Key) ->
    lists:flatten(io_lib:format("~p", [Key])).

unique_connections() ->
        dict:to_list(lists:foldl(
                fun({Key, Val}, Dict) ->
                        dict:append_list(Key, [Val], Dict)
                end, dict:new(), ets:tab2list(erlmc_connections))).

unique_connection(Host, Port) ->
    case ets:lookup(erlmc_connections, {Host, Port}) of
        [] -> exit({erlmc, {connection_not_found, {Host, Port}}});
        Pids ->
            {_, Pid} = lists:nth(random:uniform(length(Pids)), Pids),
            Pid
    end.

%% Consistent hashing functions
%%
%% First, hash memcached servers to unsigned integers on a continuum. To
%% map a key to a memcached server, hash the key to an unsigned integer
%% and locate the next largest integer on the continuum. That integer
%% represents the hashed server that the key maps to.
%% reference: http://www8.org/w8-papers/2a-webserver/caching/paper2.html
hash_to_uint(Key) when is_list(Key) -> 
    <<Int:128/unsigned-integer>> = erlang:md5(Key), Int.

%% @spec map_key(Key) -> Conn
%%               Key = string()
%%               Conn = pid()
-spec map_key(list()) -> pid().
map_key(Key) when is_list(Key) ->
    {Host, Port} =
        case
            ets:select(erlmc_continuum,[{{'$1','$2'},[{'>','$1',hash_to_uint(Key)}],['$2']}],1) of
            {[Val],_} ->
                Val;
            '$end_of_table' ->
                case ets:first(erlmc_continuum) of
                    '$end_of_table' -> exit(erlmc_continuum_empty);
                    First ->
                        [{_, Val}] = ets:lookup(erlmc_continuum, First),
                        Val
                end
        end,
    unique_connection(Host, Port).

