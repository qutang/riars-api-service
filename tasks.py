import logging
from padar_realtime.metawear_stream import MetaWearStream
import asyncio
import threading


def run_sensor(state_holder, stop_signal, device):
    def stop_sensor():
        while not stop_signal.is_set():
            pass
        device.stop()
        local_loop.call_soon_threadsafe(local_loop.stop)
        state_holder.value = 'stopped'

    local_loop = asyncio.new_event_loop()
    device = MetaWearStream(
        local_loop,
        address=device.address,
        name=device.name,
        order=device.order,
        host=device.host,
        port=device.port,
        accel_sr=device.sr,
        accel_grange=device.grange)
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