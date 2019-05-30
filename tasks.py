import logging
from padar_realtime.metawear_stream import MetaWearStream
from padar_realtime.processor_stream import ProcessorStreamManager
import asyncio
import threading
import importlib


def run_sensor(state_holder, stop_signal, d):
    async def stop_sensor():
        def stop_loop():
            local_loop.stop()
            state_holder.value = 'stopped'

        while not stop_signal.is_set():
            await asyncio.sleep(1)
        device.stop()
        local_loop.call_soon_threadsafe(stop_loop)

    local_loop = asyncio.new_event_loop()
    device = MetaWearStream(
        local_loop,
        address=d.address,
        name=d.name,
        order=d.order,
        host=d.host,
        port=d.port,
        accel_sr=d.sr,
        accel_grange=d.grange)
    try:
        device.run_ws()
        device.start()
        state_holder.value = 'running'
        local_loop.create_task(stop_sensor())
        local_loop.run_forever()
    except Exception as e:
        logging.info(str(e))
    finally:
        device.stop()
        if local_loop.is_running():
            local_loop.stop()
            state_holder.value = 'stopped'


def run_processor(state_holder, stop_signal, p, logging_folder):
    async def stop_processor():
        def stop_loop():
            local_loop.stop()
            state_holder.value = 'stopped'

        while not stop_signal.is_set() and not processor._ready_to_stop_loop:
            await asyncio.sleep(1)
        if stop_signal.is_set():
            processor.stop()
            while not processor._ready_to_stop_loop:
                await asyncio.sleep(1)
        print('stopping loop')
        local_loop.call_soon_threadsafe(stop_loop)

    local_loop = asyncio.new_event_loop()
    number_of_windows = p.number_of_windows
    processor = ProcessorStreamManager(
        local_loop,
        init_input_port=None,
        init_output_port=None,
        window_size=p.window_size,
        update_rate=p.update_rate,
        name=p.name,
        number_of_windows=number_of_windows)
    input_urls = p.device_urls
    for input_url in input_urls:
        host, port = p.get_parsed_url(input_url)
        processor.add_input_stream(host=host, port=port)
    pipeline = importlib.import_module('processors.muss')
    processor.add_processor_stream(
        pipeline_func=pipeline.entry,
        host=p.host,
        port=p.port,
        ws_server=True,
        model_name=p.name,
        logging_folder=logging_folder,
        number_of_windows=number_of_windows)
    try:
        processor.start()
        state_holder.value = 'running'
        local_loop.create_task(stop_processor())
        local_loop.run_forever()
    except Exception as e:
        logging.error(str(e))
        print(str(e))
    finally:
        print('stop process in finally')
        if local_loop.is_running():
            processor.stop()
            local_loop.call_soon_threadsafe(local_loop.stop)
            state_holder.value = 'stopped'
