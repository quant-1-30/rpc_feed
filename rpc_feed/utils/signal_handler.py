import asyncio

def _shutdown_handler(loop):
    print("Signal received, cancelling tasks...")
    for task in asyncio.all_tasks(loop):
        task.cancel()
