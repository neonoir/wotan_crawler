-module(wotan_crawler_job).

-compile(export_all).

-record(task, {
	  mod :: module(),
	  func :: function(),
	  args :: [any()]}).
-type task() :: #task{}.


setup(NWorker) ->
    Tasks = [#task{mod = wotan_crawler,
		   func = manager,
		   args = wotan_crawler:start_urls()}],
    Tasks ++ [#task{mod = wotan_crawler, 
		    func = worker, 
		    args = []} || _ <- lists:seq(1, NWorker)].
