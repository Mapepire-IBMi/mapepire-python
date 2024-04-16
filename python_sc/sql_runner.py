import asyncio

class SQLJobRunner:
    def run(self, coro):
        """ Run a coroutine and return the result, handling the event loop. """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:  # No running loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(coro)
            loop.close()
            return result
        else:
            return loop.run_until_complete(coro)
