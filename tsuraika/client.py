import asyncio
import sys
import msgpack
import json
import os
from .common import ClientConfig, MessageType, validate_message, logger


class ProxyClient:
    def __init__(self, config: ClientConfig):
        self.config = config
        self.local_connections = {}
        self.server_writer = None
        self.heartbeat_interval = 60
        self.last_server_heartbeat = 0
        self.connection_lost = False
        self.is_closing = False

    def decode_if_bytes(self, value):
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return value

    def get_message_field(self, message: dict, field: str):
        return message.get(field.encode()) or message.get(field)

    def get_data_field(self, data: dict, field: str):
        return data.get(field.encode()) or data.get(field)

    async def forward_local_to_server(
        self, reader: asyncio.StreamReader, connection_id: str
    ):
        try:
            while True:
                data = await reader.read(8192)
                if not data:
                    break

                if self.server_writer:
                    message = {
                        "type": MessageType.DATA.value,
                        "data": {
                            "proxy_name": self.config.proxy_name,
                            "data": data,
                            "connection_id": connection_id,
                        },
                    }
                    logger.verbose(f"Forwarding local data to server: {message}")
                    packed_data = msgpack.packb(message)
                    self.server_writer.write(packed_data)
                    await self.server_writer.drain()

        except Exception as e:
            logger.error(f"Error forwarding local data to server: {e}")
        finally:
            if connection_id in self.local_connections:
                reader, writer = self.local_connections[connection_id]
                writer.close()
                await writer.wait_closed()
                del self.local_connections[connection_id]

    async def create_local_connection(
        self, connection_id: str
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        try:
            reader, writer = await asyncio.open_connection(
                self.config.local_addr, self.config.local_port
            )
            self.local_connections[connection_id] = (reader, writer)
            logger.debug(
                f"Created new local connection {connection_id} to {self.config.local_addr}:{self.config.local_port}"
            )

            asyncio.create_task(self.forward_local_to_server(reader, connection_id))

            return reader, writer
        except Exception as e:
            logger.error(f"Failed to create local connection: {e}")
            raise

    async def handle_server_heartbeat(self):
        while True:
            current_time = asyncio.get_event_loop().time()
            if current_time - self.last_server_heartbeat > self.heartbeat_interval:
                await self.send_heartbeat()
            await asyncio.sleep(1)

    async def send_heartbeat(self):
        if self.server_writer:
            message = {
                "type": MessageType.CLIENT_HEARTBEAT.value,
                "data": {
                    "proxy_name": self.config.proxy_name,
                },
            }
            self.server_writer.write(msgpack.packb(message))
            await self.server_writer.drain()
            logger.debug("Sent heartbeat to server")

    async def handle_server_data(self, reader: asyncio.StreamReader):
        unpacker = msgpack.Unpacker()
        try:
            while True:
                try:
                    packed_data = await asyncio.wait_for(
                        reader.read(8192), timeout=self.heartbeat_interval + 5
                    )
                    if not packed_data:
                        logger.warning("Server closed the connection")
                        return

                    unpacker.feed(packed_data)
                    for message in unpacker:
                        try:
                            if not validate_message(message):
                                logger.error(
                                    "Invalid message format received from server"
                                )
                                continue

                            msg_type = self.decode_if_bytes(
                                self.get_message_field(message, "type")
                            )

                            if msg_type == MessageType.DATA.value:
                                logger.verbose(f"Received data message: {message}")
                            else:
                                logger.debug(f"Received message: {message}")

                            data = self.get_message_field(message, "data")

                            if msg_type == MessageType.INITIAL_RESPONSE.value:
                                remote_port = self.get_data_field(data, "remote_port")
                                proxy_name = self.decode_if_bytes(
                                    self.get_data_field(data, "proxy_name")
                                )
                                logger.info(
                                    f"Proxy established on remote port: {remote_port} for {proxy_name}"
                                )

                            elif msg_type == MessageType.DATA.value:
                                proxy_name = self.decode_if_bytes(
                                    self.get_data_field(data, "proxy_name")
                                )
                                if proxy_name == self.config.proxy_name:
                                    binary_data = self.get_data_field(data, "data")
                                    connection_id = self.decode_if_bytes(
                                        self.get_data_field(data, "connection_id")
                                    )

                                    if connection_id not in self.local_connections:
                                        _, writer = await self.create_local_connection(
                                            connection_id
                                        )
                                    else:
                                        _, writer = self.local_connections[
                                            connection_id
                                        ]

                                    writer.write(binary_data)
                                    await writer.drain()
                                    logger.debug(
                                        f"Forwarded data to local service for connection {connection_id}"
                                    )

                            elif msg_type == MessageType.SERVER_HEARTBEAT.value:
                                self.heartbeat_interval = self.get_data_field(
                                    data, "heartbeat_interval"
                                )
                                self.last_server_heartbeat = (
                                    asyncio.get_event_loop().time()
                                )
                                logger.debug(
                                    f"Received server heartbeat, interval: {self.heartbeat_interval}"
                                )
                                continue

                            elif msg_type == MessageType.CLEANUP_RESPONSE.value:
                                proxy_name = self.decode_if_bytes(
                                    self.get_data_field(data, "proxy_name")
                                )
                                status = self.decode_if_bytes(
                                    self.get_data_field(data, "status")
                                )
                                if proxy_name == self.config.proxy_name:
                                    if status == "success":
                                        logger.info(
                                            f"Cleanup request success for {proxy_name}"
                                        )
                                    else:
                                        logger.warning(
                                            f"Cleanup request failed for {proxy_name}"
                                        )
                                else:
                                    logger.warning(
                                        f"Received cleanup response for unknown proxy: {proxy_name}"
                                    )
                                continue

                        except Exception as e:
                            logger.error(f"Error processing message: {e}")
                            logger.exception(e)

                except asyncio.CancelledError:
                    logger.info("Server data handling task cancelled")
                    logger.info("Cleaning up and exiting client...")
                    self.is_closing = True
                    await self.cleanup()
                    for task in asyncio.all_tasks():
                        if task is not asyncio.current_task():
                            task.cancel()
                    await asyncio.gather(*asyncio.all_tasks(), return_exceptions=True)
                    return

                except asyncio.TimeoutError:
                    logger.warning(
                        "No data received from server for an extended period"
                    )
                    return

        except (
            ConnectionResetError,
            ConnectionAbortedError,
            asyncio.IncompleteReadError,
        ) as e:
            logger.error(f"Connection error: {e}")
        except msgpack.exceptions.UnpackException as e:
            logger.error(f"Unpack error: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in handle_server_data: {e}")
            logger.exception(e)
        finally:
            for connection_id, (_, writer) in self.local_connections.items():
                writer.close()
                await writer.wait_closed()
            self.local_connections.clear()

    async def cleanup(self):
        if self.server_writer and not self.connection_lost:
            cleanup_request = {
                "type": MessageType.CLEANUP_REQUEST.value,
                "data": {
                    "proxy_name": self.config.proxy_name,
                },
            }
            self.server_writer.write(msgpack.packb(cleanup_request))
            await self.server_writer.drain()

            try:
                cleanup_response = await asyncio.wait_for(
                    self.reader.read(8192), timeout=5.0
                )
                unpacked_response = msgpack.unpackb(cleanup_response)
                if (
                    self.get_message_field(unpacked_response, "type")
                    == MessageType.CLEANUP_RESPONSE.value
                ):
                    logger.info("Cleanup request acknowledged by server")
                else:
                    logger.warning("Unexpected response to cleanup request")
            except asyncio.TimeoutError:
                logger.warning("Timeout waiting for cleanup response from server")

        for connection_id, (_, writer) in self.local_connections.items():
            writer.close()
            await writer.wait_closed()
        self.local_connections.clear()

        if self.server_writer and not self.connection_lost:
            logger.info("Closing server writer")
            self.server_writer.close()
            logger.info("Server writer closed")
            sys.exit(0)

    async def connect_to_server(self):
        while True:
            try:
                self.reader, self.server_writer = await asyncio.open_connection(
                    self.config.server_addr, self.config.server_port
                )
                logger.info(
                    f"Connected to server at {self.config.server_addr}:{self.config.server_port}"
                )
                return
            except Exception as e:
                if self.connection_lost:
                    logger.info("Connection lost, retry in 3 seconds...")
                    await asyncio.sleep(3)
                else:
                    logger.error(
                        f"Failed to connect to server: {e}, retry in 30 seconds..."
                    )
                    await asyncio.sleep(30)

    async def start(self):
        while True:
            try:
                logger.info("Connecting to server...")

                await self.connect_to_server()

                initial_request = {
                    "type": MessageType.INITIAL_REQUEST.value,
                    "data": {
                        "proxy_type": "tcp",
                        "remote_port": self.config.remote_port,
                        "proxy_name": self.config.proxy_name,
                    },
                }

                self.server_writer.write(msgpack.packb(initial_request))
                await self.server_writer.drain()

                heartbeat_task = asyncio.create_task(self.handle_server_heartbeat())

                try:
                    await self.handle_server_data(self.reader)
                except Exception as e:
                    logger.error(f"Error in handle_server_data: {e}")
                    logger.exception(e)
                finally:
                    heartbeat_task.cancel()
                    try:
                        await heartbeat_task
                    except asyncio.CancelledError:
                        pass

                logger.info("Connection to server lost, attempting to reconnect...")
            except Exception as e:
                logger.error(f"Connection to server lost: {e}, retry in 3 seconds...")
                logger.exception(e)
            finally:
                if not self.is_closing:
                    logger.info("Attempting to reconnect in 3 seconds...")
                    self.connection_lost = True
                    try:
                        if self.server_writer:
                            self.server_writer.close()
                            await self.server_writer.wait_closed()
                        await self.cleanup()
                        await asyncio.sleep(3)
                    except Exception as e:
                        logger.error(f"Error in cleanup: {e}")
                        logger.exception(e)
                else:
                    logger.info("Client is closing, exiting")


def load_config(config_path: str) -> ClientConfig:
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r") as f:
        config_data = json.load(f)

    return ClientConfig(
        server_addr=config_data.get("server_addr", "127.0.0.1"),
        server_port=config_data.get("server_port", 7000),
        local_addr=config_data.get("local_addr", "127.0.0.1"),
        local_port=config_data.get("local_port", 80),
        proxy_name=config_data.get("proxy_name", "test_proxy"),
        remote_port=config_data.get("remote_port", 8080),
    )
