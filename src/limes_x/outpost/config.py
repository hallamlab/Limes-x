from hypercorn.config import Config

HOST, PORT = "localhost", 12100
VER = "v1"

def GetConfig():
    config = Config()
    config.bind = [f"{HOST}:{PORT}"]
    return config
