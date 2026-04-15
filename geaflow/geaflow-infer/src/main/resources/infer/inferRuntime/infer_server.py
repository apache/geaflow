#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import argparse
import importlib
import os
import signal
import sys
import threading
import time
import traceback
from inferSession import TorchInferSession
from pickle_bridge import PicklerDataBridger

class check_ppid(threading.Thread):
    def __init__(self, name, daemon):
        super().__init__(name=name, daemon=daemon)

    def run(self) -> None:
        while os.getppid() != 1:
            time.sleep(1)
        os.kill(os.getpid(), signal.SIGKILL)

def parse_dir_script(script_path):
    index = str(script_path).rindex('/')
    dir_str = script_path[0: index + 1]
    script_name = script_path[index + 1: len(script_path) - 3]
    return dir_str, script_name


def get_user_define_class(class_name):
    transform_path = os.getcwd() + "/TransFormFunctionUDF.py"
    dir_name = parse_dir_script(transform_path)
    sys.path.insert(0, dir_name[0])
    user_py = importlib.import_module(dir_name[1])
    if hasattr(user_py, class_name):
        transform_class = getattr(user_py, class_name)
        return transform_class()
    else:
        raise ValueError("class name = {} not found".format(class_name))


def start_infer_process(class_name, output_queue_shm_id, input_queue_shm_id,
                        model_path=None, model_version_file=None,
                        poll_interval_sec=1.0, backoff_sec=10.0,
                        warmup_enabled=True, hot_reload_enabled=True):
    transform_class = get_user_define_class(class_name)
    hot_reload_options = {
        "model_path": model_path,
        "model_version_file": model_version_file,
        "poll_interval_sec": poll_interval_sec,
        "backoff_sec": backoff_sec,
        "warmup_enabled": warmup_enabled,
        "hot_reload_enabled": hot_reload_enabled,
    }
    infer_session = TorchInferSession(transform_class, hot_reload_options)
    input_size = transform_class.input_size
    data_exchange = PicklerDataBridger(input_queue_shm_id, output_queue_shm_id, input_size)
    check_thread = check_ppid('check_process', True)
    check_thread.start()
    count = 0
    while True:
        try:
            inputs = data_exchange.read_data()
            if not inputs:
                count += 1
                if count % 1000 == 0:
                    time.sleep(0.05)
                    count = 0
            else:
                res = infer_session.run(*inputs)
                data_exchange.write_data(res)
        except Exception as e:
            exc_type, exc_val, exc_tb = sys.exc_info()
            error_msg = "".join(traceback.format_exception(exc_type, exc_val, exc_tb))
            data_exchange.write_data('python_exception: ' + error_msg)
            sys.exit(0)


def _str2bool(value):
    return str(value).lower() in ("1", "true", "t", "yes", "y", "on")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tfClassName", type=str,
                        help="user define transformer class name")
    parser.add_argument("--input_queue_shm_id", type=str, help="input queue "
                                                               "share memory "
                                                               "id")
    parser.add_argument("--output_queue_shm_id", type=str,
                        help="output queue share memory id")
    parser.add_argument("--model_path", type=str,
                        default=os.path.join(os.getcwd(), "model.pt"),
                        help="model file path")
    parser.add_argument("--model_version_file", type=str,
                        default=os.path.join(os.getcwd(), "model.version"),
                        help="manifest file path")
    parser.add_argument("--poll_interval_sec", type=float, default=1.0,
                        help="manifest poll interval in seconds")
    parser.add_argument("--backoff_sec", type=float, default=10.0,
                        help="reload backoff in seconds after failure")
    parser.add_argument("--warmup_enabled", type=_str2bool, default=True,
                        help="enable dummy warmup before switching")
    parser.add_argument("--hot_reload_enabled", type=_str2bool, default=True,
                        help="enable model hot reload")
    args = parser.parse_args()
    start_infer_process(args.tfClassName, args.output_queue_shm_id,
                        args.input_queue_shm_id, args.model_path,
                        args.model_version_file, args.poll_interval_sec,
                        args.backoff_sec, args.warmup_enabled,
                        args.hot_reload_enabled)
