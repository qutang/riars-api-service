import logging
from padar_realtime.metawear_stream import MetaWearStream
from padar_realtime.processor_stream import ProcessorStreamManager
import asyncio
import threading
import importlib


def run_sensor(state_holder, stop_signal, d):
    def stop_sensor():
        while not stop_signal.is_set():
            pass
        device.stop()
        local_loop.call_soon_threadsafe(local_loop.stop)
        state_holder.value = 'stopped'

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
        threading.Thread(target=stop_sensor).start()
        local_loop.run_forever()
    except Exception as e:
        logging.info(str(e))
    finally:
        device.stop()
        local_loop.stop()
        state_holder.value = 'stopped'


def run_processor(state_holder, stop_signal, p):
    def stop_processor():
        while not stop_signal.is_set():
            pass
        processor.stop()
        while not processor._ready_to_stop_loop:
            pass
        state_holder.value = 'stopped'

    local_loop = asyncio.new_event_loop()
    processor = ProcessorStreamManager(
        local_loop,
        init_input_port=None,
        init_output_port=None,
        window_size=p.window_size,
        update_rate=p.update_rate,
        name=p.name)
    input_urls = p.device_urls
    for input_url in input_urls:
        host, port = p.get_parsed_url(input_url)
        processor.add_input_stream(host=host, port=port)
    pipeline = importlib.import_module('processors.' + p.name)
    processor.add_processor_stream(
        pipeline_func=pipeline.entry, host=p.host, port=p.port, ws_server=True)
    try:
        processor.start()
        state_holder.value = 'running'
        threading.Thread(target=stop_processor).start()
        local_loop.run_forever()
    except Exception as e:
        logging.error(str(e))
        print(str(e))
    finally:
        processor.stop()
        local_loop.call_soon_threadsafe(local_loop.stop)
        state_holder.value = 'stopped'
