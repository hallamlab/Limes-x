from hypercorn.config import Config

ui_config = Config()
ui_config.bind = ["localhost:12099"]
