-module(wotan_crawler).

-compile(export_all).

-record(request, {
	  method = get :: atom(),
	  url :: binary(),
	  headers = [] :: list(),
	  payload = <<"">> :: binary(),
	  options = [] :: list()}).
-type request() :: #request{}.
-record(crawler_task, {
	  mod :: module(),
	  func :: fun(),
	  request :: request()}).

crawler_name() ->
    <<"wotan_test_crawler">>.

initial_tasks() ->
    [#crawler_task{
	mod = wotan_crawler_parser,
	func = parse,
	request = #request{url = <<"www.example.com">>}}].

manager_init() ->
    Channel = wotan_rmq_utils:get_channel("localhost"),
    WQueueName = <<(crawler_name())/binary ,<<"_task_queue">>/bianry>>,
    WorkerQueue = wotan_rmq_utils:declare_worker_queue(Channel, WQueueName),
    MQueueName = <<(crawler_name())/binary ,<<"_manager_queue">>/bianry>>,
    ManagerQueue = wotan_rmq_utils:declare_worker_queue(Channel, MQueueName),
    %% start listening to manager_queue for messages from workers
    wotan_rmq_utils:subscribe(Channel, ManagerQueue),
    
    SeenURLS = assign_tasks(Channel, WQueueName, initial_tasks()),

    manager_loop(Channel, WQueueName, SeenURLS).

manager_loop(Channel, WQueueName, SeenURLS) ->
    receive
	{#'basic.deliver'{delivery_tag = Tag}, #amqp_msg{payload = Msg}} ->
	    NewTasks = remove_duplicate_urls(binary_to_term(Msg)),
	    SeenURLS2 = assign_tasks(Channel, WQueueName, NewTasks),
	    wotan_rmq_utils:ack(Channel, Tag),
	    manager_loop(Channel, WQueueName, SeenURLS2 ++ SeenURLS)
    end.

downloader_init() ->
    ensure_started(hackney),
    
    Channel = wotan_rmq_utils:get_channel("localhost"),
    WQueueName = <<(crawler_name())/binary ,<<"_task_queue">>/bianry>>,
    WorkerQueue = wotan_rmq_utils:declare_worker_queue(Channel, WQueueName),
    MQueueName = <<(crawler_name())/binary ,<<"_manager_queue">>/bianry>>,
    ManagerQueue = wotan_rmq_utils:declare_worker_queue(Channel, MQueueName),
    %% start listening to worker_queue for messages from managers
    wotan_rmq_utils:subscribe(Channel, WorkerQueue),
    
    downloader_loop(Channel, MQueueName).

downloader_loop(Channel, MQueueName) ->
    receive
	{#'basic.deliver'{delivery_tag = Tag}, #amqp_msg{payload = Msg}} ->
	    #crawler_task{mod = M,
			  func = F,
			  request = 
			      #request{method = Method,
				       url = URL,
				       headers = Headers,
				       payload = Payload,
				       options = Options}} = 
		term_to_binary(Msg),

	    {ok, StatusCode, RespHeaders, ClientRef} = 
		hackney:request(Method, URL, Headers, Payload, Options),
	    {ok, Body} = hackney:body(ClientRef),
	    NewTasks = apply(M, F, Body),
	    
	    wotan_rmq_utils:publish(Channel, <<"">>, MQueueName, NewTasks),
	    
	    wotan_rmq_utils:ack(Channel, Tag),
	    downloader_loop(Channel, MQueueName)
    end.




assign_tasks(Channel, Queue, Tasks) ->
    assign_tasks(Channel, Queue, Tasks, []).

assign_tasks(Channel, Queue, [], Acc) ->
    Acc;
assign_tasks(Channel, Queue, [Task|Tasks], Acc) ->
    #crawler_task{request = Request} = Task,
    URL = get_request_url(Request),
    assign_task(Channel, Queue, Task),
    assign_tasks(Channel, Queue, Tasks, [URL|Acc].

assign_task(Channel, Queue, Task) ->
    wotan_rmq_utils:publish(Channel, <<"">>, Queue, Task).

get_task_url(#crawler_task{request = Request} = Task) ->
    get_request_url(Request).

get_request_url(#request{url = URL} = Request) ->
    URL.

remove_duplicate_urls(Tasks, SeenURLS) ->
    [Task || Task <- Tasks, not lists:member(get_task_url(Task), SeenURLS)].	

ensure_started(App) ->
    case application:start(App) of
        ok ->
            ok;
        {error, {already_started, App}} ->
            ok
    end.
