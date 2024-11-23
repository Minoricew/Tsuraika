import logging
from enum import Enum
from dataclasses import dataclass
import colorama

colorama.init()


class Colors:
    class Basic:
        TIME = "\033[92m"
        SOURCE = "\033[93m"
        INFO = "\033[94m"
        WARNING = "\033[32m"
        ERROR = "\033[91m"
        DEBUG = "\033[33m"
        VERBOSE = "\033[95m"
        RESET = "\033[0m"

    class Hex:
        TIME = "\033[38;2;144;238;144m"
        SOURCE = "\033[38;2;180;149;212m"
        INFO = "\033[38;2;102;204;255m"
        WARNING = "\033[38;2;251;129;144m"
        ERROR = "\033[38;2;229;61;48m"
        DEBUG = "\033[38;2;137;137;255m"
        VERBOSE = "\033[38;2;119;188;195m"
        RESET = "\033[0m"


class ColoredFormatter(logging.Formatter):
    def __init__(self, use_basic_colors=False):
        super().__init__()
        self.colors = Colors.Basic if use_basic_colors else Colors.Hex
        self.default_formatter = logging.Formatter(
            f"{self.colors.TIME}%(asctime)s{self.colors.RESET} - "
            f"{self.colors.SOURCE}%(name)s{self.colors.RESET} - "
            "%(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        self.formatters = {
            logging.INFO: logging.Formatter(
                f"{self.colors.TIME}%(asctime)s{self.colors.RESET} - "
                f"{self.colors.SOURCE}%(name)s{self.colors.RESET} - "
                f"{self.colors.INFO}%(levelname)s - %(message)s{self.colors.RESET}",
                datefmt="%Y-%m-%d %H:%M:%S",
            ),
            logging.WARNING: logging.Formatter(
                f"{self.colors.TIME}%(asctime)s{self.colors.RESET} - "
                f"{self.colors.SOURCE}%(name)s{self.colors.RESET} - "
                f"{self.colors.WARNING}%(levelname)s - %(message)s{self.colors.RESET}",
                datefmt="%Y-%m-%d %H:%M:%S",
            ),
            logging.ERROR: logging.Formatter(
                f"{self.colors.TIME}%(asctime)s{self.colors.RESET} - "
                f"{self.colors.SOURCE}%(name)s{self.colors.RESET} - "
                f"{self.colors.ERROR}%(levelname)s - %(message)s{self.colors.RESET}",
                datefmt="%Y-%m-%d %H:%M:%S",
            ),
            logging.DEBUG: logging.Formatter(
                f"{self.colors.TIME}%(asctime)s{self.colors.RESET} - "
                f"{self.colors.SOURCE}%(name)s{self.colors.RESET} - "
                f"{self.colors.DEBUG}%(levelname)s - %(message)s{self.colors.RESET}",
                datefmt="%Y-%m-%d %H:%M:%S",
            ),
            VERBOSE: logging.Formatter(
                f"{self.colors.TIME}%(asctime)s{self.colors.RESET} - "
                f"{self.colors.SOURCE}%(name)s{self.colors.RESET} - "
                f"{self.colors.VERBOSE}%(levelname)s - %(message)s{self.colors.RESET}",
                datefmt="%Y-%m-%d %H:%M:%S",
            ),
        }

    def format(self, record):
        formatter = self.formatters.get(record.levelno, self.default_formatter)
        return formatter.format(record)


VERBOSE = 5
logging.addLevelName(VERBOSE, "VERBOSE")


class VerboseLogger(logging.Logger):
    def verbose(self, message, *args, **kwargs):
        if self.isEnabledFor(VERBOSE):
            self._log(VERBOSE, message, args, **kwargs)


logging.setLoggerClass(VerboseLogger)


def setup_logging(use_basic_colors=False):
    handler = logging.StreamHandler()
    handler.setFormatter(ColoredFormatter(use_basic_colors))

    logging.basicConfig(level=logging.INFO, handlers=[handler])

    return logging.getLogger("tsuraika")


logger = logging.getLogger("tsuraika")


class MessageType(Enum):
    INITIAL_REQUEST = "initial_request"
    INITIAL_RESPONSE = "initial_response"
    DATA = "data"
    CLEANUP_REQUEST = "cleanup_request"
    CLEANUP_RESPONSE = "cleanup_response"
    SERVER_HEARTBEAT = "server_heartbeat"
    CLIENT_HEARTBEAT = "client_heartbeat"


@dataclass
class ClientConfig:
    server_addr: str
    server_port: int
    local_addr: str
    local_port: int
    proxy_name: str
    remote_port: int


def validate_message(message: dict) -> bool:
    """Validate message format"""
    try:
        if not isinstance(message, dict):
            return False

        type_key = b"type" if isinstance(next(iter(message.keys())), bytes) else "type"
        data_key = b"data" if isinstance(next(iter(message.keys())), bytes) else "data"

        if type_key not in message or data_key not in message:
            return False

        if not isinstance(message[data_key], dict):
            return False

        msg_type = message[type_key]
        if isinstance(msg_type, bytes):
            msg_type = msg_type.decode("utf-8")

        data = message[data_key]

        if msg_type == MessageType.INITIAL_REQUEST.value:
            required_fields = (
                {b"proxy_type", b"remote_port", b"proxy_name"}
                if isinstance(next(iter(data.keys())), bytes)
                else {"proxy_type", "remote_port", "proxy_name"}
            )
            return all(field in data for field in required_fields)

        elif msg_type == MessageType.INITIAL_RESPONSE.value:
            required_fields = (
                {b"proxy_name", b"remote_port"}
                if isinstance(next(iter(data.keys())), bytes)
                else {"proxy_name", "remote_port"}
            )
            return all(field in data for field in required_fields)

        elif msg_type == MessageType.DATA.value:
            required_fields = (
                {b"proxy_name", b"data"}
                if isinstance(next(iter(data.keys())), bytes)
                else {"proxy_name", "data"}
            )
            return all(field in data for field in required_fields)

        elif msg_type == MessageType.CLEANUP_REQUEST.value:
            required_fields = (
                {b"proxy_name"}
                if isinstance(next(iter(data.keys())), bytes)
                else {"proxy_name"}
            )
            return all(field in data for field in required_fields)

        elif msg_type == MessageType.CLEANUP_RESPONSE.value:
            required_fields = (
                {b"proxy_name", b"status"}
                if isinstance(next(iter(data.keys())), bytes)
                else {"proxy_name", "status"}
            )
            return all(field in data for field in required_fields)

        elif msg_type == MessageType.CLIENT_HEARTBEAT.value:
            required_fields = (
                {b"proxy_name"}
                if isinstance(next(iter(data.keys())), bytes)
                else {"proxy_name"}
            )
            return all(field in data for field in required_fields)

        elif msg_type == MessageType.SERVER_HEARTBEAT.value:
            required_fields = (
                {b"heartbeat_interval"}
                if isinstance(next(iter(data.keys())), bytes)
                else {"heartbeat_interval"}
            )
            return all(field in data for field in required_fields)

        return False
    except Exception as e:
        logger.error(f"Validation error: {e}")
        return False
