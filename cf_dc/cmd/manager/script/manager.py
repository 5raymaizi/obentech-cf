#!/usr/bin/python3

import requests
import argparse
import os
import yaml

'''
一个简单脚本用于调用manager rest api
'''

conf = {
    "queue": {
        "msecs": 1000,
        "open_threshold": 1.1,
        "close_threshold": -2.0
    },

    "open_allowed": False,
    "close_allowed": False,
}

addr = ""
coin = "doge"

class CFSg:
    prefix:str

    @classmethod
    def init(cls, addr: str):
        cls.prefix = "http://{}/api/cf_arbitrage".format(addr)
    
    @classmethod
    def keys(cls):
        url = "{}/keys".format(cls.prefix)
        js = requests.get(url)
        print(js.json())
    
    @classmethod
    def stopall(cls):
        url = "{}/stopall".format(cls.prefix)
        js = requests.put(url)
        print(js.json())

    @classmethod
    def startall(cls):
        url = "{}/startall".format(cls.prefix)
        js = requests.put(url)
        print(js.json())

    @classmethod
    def refreshall(cls):
        url = "{}/refreshall".format(cls.prefix)
        js = requests.put(url)
        print(js.json())

    def __init__(self, coin: str):
        self.coin = coin
        self.url = "{}/{}".format(self.prefix, coin)
    
    def request(self, method, url, *args, **kwargs):
        cb = getattr(requests, method)
        resp = cb(url, *args, **kwargs)
        if resp.status_code != 200:
            raise ValueError("reqeust error method={} url={} resp={}".format(method, url, resp.text))
        print(resp.text)
    
    def add(self):
        self.request("post", self.url, json=conf)

    def addOrder(self):
        headers = {"operator":"test"}
        params = {
            "type": "spot",
            "side": "buy",
            "amount":"0.05"
        }
        url = "{}/addOrder".format(self.url)
        self.request("post", url, json=params, headers=headers)

    def transfer(self):
        headers = {"operator":"test"}
        params = {
            "dest": "spot",
            "amount":"0.05"
        }
        url = "{}/transfer".format(self.url)
        self.request("post", url, json=params, headers=headers)

    def get(self):
        self.request("get", self.url)
    
    def stop(self):
        url = "{}/stop".format(self.url)
        self.request("put", url)
    
    def start(self):
        url = "{}/start".format(self.url)
        self.request("put", url)

    def put(self):
        conf["queue"]["msecs"] += 1
        self.request("put", self.url, json=conf)

    def clear(self):
        url = "{}/clear".format(self.url)
        self.request("put", url)

    def delete(self):
        url = "{}/del".format(self.url)
        self.request("delete", url)

def build_method(method):
    def cb(args):
        CFSg.init(args.addr)

        class_method = ["keys", "stopall", "startall", "refreshall"]
        if method in class_method :
            m = getattr(CFSg, method)
            m()
            return
        

        if not args.coin:
            raise ValueError("-c is required")
        sg = CFSg(args.coin)

        m = getattr(sg, method)
        return m()
    
    return cb

if __name__ == "__main__":
    # 获取脚本所在目录的绝对路径
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)  # 上级目录
    
    # 定义可能存在的配置文件路径（优先上级目录）
    config_paths = [
        os.path.join(parent_dir, 'conf/config.yaml'),  # 先找上级conf目录
        os.path.join(script_dir, 'config.yaml')   # 再找同级目录
    ]
    
    port = '14399'  # 默认端口
    found_config = False
    for config_path in config_paths:
        if os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    config = yaml.safe_load(f)
                    bind_value = config.get('bind', '')
                    if bind_value:
                        # 分割bind值以获取端口（处理格式":14300"或"0.0.0.0:14300"）
                        parts = bind_value.rsplit(':', 1)
                        port_part = parts[1] if len(parts) == 2 else parts[0]
                        port = port_part if port_part else '14399'
                        found_config = True
                        break  # 找到有效配置后退出循环
            except Exception as e:
                print(f"Warning: Failed to parse {config_path}, using default port 14399. Error: {e}")
                break  # 如果文件存在但解析失败，不再尝试其他路径
    # 如果所有配置文件都不存在或解析失败，使用默认端口
    default_addr = f"127.0.0.1:{port}"

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-c", dest="coin", type=str, default="", help="specific operate coin")
    parser.add_argument("-a", dest="addr", type=str, default=default_addr, help="specific manaager addr host:port")

    sub = parser.add_subparsers()

    methods = ["add", "get", "stop", "start", "keys", "startall", "stopall", "addOrder", "tranfer", "clear", "refreshall", "delete"]
    for m in methods:
        p = sub.add_parser(m)
        p.set_defaults(func=build_method(m))

    op = parser.parse_args()
    op.func(op)
