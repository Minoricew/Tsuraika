import asyncio
import msgpack
from typing import Dict
from .common import MessageType, validate_message, logger


class ProxyServer:
    def __init__(self, server_port: int, heartbeat_interval: int = 60):
        self.server_port = server_port
        self.proxy_servers: Dict[str, asyncio.Server] = {}
        self.proxy_clients: Dict[str, asyncio.StreamWriter] = {}
        self.remote_connections: Dict[str, Dict[str, asyncio.StreamWriter]] = {}
        self.client_heartbeats = {}
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = 10

    def decode_if_bytes(self, value):
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return value

    def get_message_field(self, message: dict, field: str):
        return message.get(field.encode()) or message.get(field)

    def get_data_field(self, data: dict, field: str):
        return data.get(field.encode()) or data.get(field)

    async def handle_remote_connection(
        self,
        proxy_name: str,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ):
        client_addr = writer.get_extra_info("peername")
        connection_id = f"{proxy_name}_{client_addr[0]}_{client_addr[1]}"
        logger.debug(f"New remote connection from {client_addr} for proxy {proxy_name}")

        if proxy_name not in self.remote_connections:
            self.remote_connections[proxy_name] = {}
        self.remote_connections[proxy_name][connection_id] = writer

        try:
            while True:
                data = await reader.read(8192)
                if not data:
                    break

                if proxy_name in self.proxy_clients:
                    message = {
                        "type": MessageType.DATA.value,
                        "data": {
                            "proxy_name": proxy_name,
                            "data": data,
                            "connection_id": connection_id,
                        },
                    }
                    packed_data = msgpack.packb(message)
                    client_writer = self.proxy_clients[proxy_name]
                    client_writer.write(packed_data)
                    await client_writer.drain()
        except Exception as e:
            logger.error(f"Remote connection error: {e}")
        finally:
            if (
                proxy_name in self.remote_connections
                and connection_id in self.remote_connections[proxy_name]
            ):
                del self.remote_connections[proxy_name][connection_id]
            writer.close()
            await writer.wait_closed()
            logger.debug(f"Remote connection closed from {client_addr}")

    async def create_remote_server(self, proxy_name: str, remote_port: int) -> bool:
        try:
            server = await asyncio.start_server(
                lambda r, w: self.handle_remote_connection(proxy_name, r, w),
                "0.0.0.0",
                remote_port,
            )
            self.proxy_servers[proxy_name] = server
            logger.info(f"Created remote server for {proxy_name} on port {remote_port}")
            return True
        except Exception as e:
            logger.error(f"Failed to create remote server: {e}")
            return False

    async def cleanup_client(self, proxy_name: str):
        if proxy_name in self.proxy_clients:
            del self.proxy_clients[proxy_name]

        if proxy_name in self.proxy_servers:
            server = self.proxy_servers[proxy_name]
            server.close()
            await server.wait_closed()
            del self.proxy_servers[proxy_name]

        if proxy_name in self.remote_connections:
            for connection_id, writer in self.remote_connections[proxy_name].items():
                writer.close()
                await writer.wait_closed()
            del self.remote_connections[proxy_name]

        if proxy_name in self.client_heartbeats:
            del self.client_heartbeats[proxy_name]

        logger.info(f"Cleaned up resources for {proxy_name}")

    async def send_heartbeat(self, writer, proxy_name):
        message = {
            "type": MessageType.SERVER_HEARTBEAT.value,
            "data": {
                "heartbeat_interval": self.heartbeat_interval,
            },
        }
        writer.write(msgpack.packb(message))
        await writer.drain()
        logger.debug(f"Sent heartbeat to client {proxy_name}")

    async def check_client_heartbeats(self):
        while True:
            await asyncio.sleep(5)
            current_time = asyncio.get_event_loop().time()
            for proxy_name, last_time in list(self.client_heartbeats.items()):
                if (
                    current_time - last_time
                    > self.heartbeat_interval + self.heartbeat_timeout
                ):
                    logger.warning(f"Client {proxy_name} heartbeat timeout")
                    await self.cleanup_client(proxy_name)

    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        client_addr = writer.get_extra_info("peername")
        logger.info(f"New client connection from {client_addr}")
        unpacker = msgpack.Unpacker()
        try:
            while True:
                packed_data = await reader.read(8192)
                if not packed_data:
                    break

                unpacker.feed(packed_data)
                for message in unpacker:
                    try:

                        if not validate_message(message):
                            logger.error("Invalid message format")
                            logger.debug(f"Invalid message: {message}")
                            continue

                        msg_type = self.decode_if_bytes(
                            self.get_message_field(message, "type")
                        )

                        if msg_type == MessageType.DATA.value:
                            logger.verbose(f"Received data message: {message}")
                        else:
                            logger.debug(f"Received message: {message}")

                        data = self.get_message_field(message, "data")

                        if msg_type == MessageType.INITIAL_REQUEST.value:
                            proxy_name = self.decode_if_bytes(
                                self.get_data_field(data, "proxy_name")
                            )
                            remote_port = self.get_data_field(data, "remote_port")

                            success = await self.create_remote_server(
                                proxy_name, remote_port
                            )

                            if success:
                                self.proxy_clients[proxy_name] = writer

                                response = {
                                    "type": MessageType.INITIAL_RESPONSE.value,
                                    "data": {
                                        "proxy_name": proxy_name,
                                        "remote_port": remote_port,
                                    },
                                }
                                writer.write(msgpack.packb(response))
                                await writer.drain()

                        elif msg_type == MessageType.DATA.value:
                            proxy_name = self.decode_if_bytes(
                                self.get_data_field(data, "proxy_name")
                            )
                            binary_data = self.get_data_field(data, "data")
                            connection_id = self.decode_if_bytes(
                                self.get_data_field(data, "connection_id")
                            )

                            if (
                                proxy_name in self.remote_connections
                                and connection_id in self.remote_connections[proxy_name]
                            ):
                                remote_writer = self.remote_connections[proxy_name][
                                    connection_id
                                ]
                                try:
                                    remote_writer.write(binary_data)
                                    await remote_writer.drain()
                                    logger.debug(
                                        f"Forwarded data to remote connection {connection_id}"
                                    )
                                except Exception as e:
                                    logger.error(
                                        f"Failed to forward data to remote connection: {e}"
                                    )
                            else:
                                logger.warning(
                                    f"No remote connection found for {proxy_name}:{connection_id}"
                                )

                        elif msg_type == MessageType.CLEANUP_REQUEST.value:
                            proxy_name = self.decode_if_bytes(
                                self.get_data_field(data, "proxy_name")
                            )
                            await self.cleanup_client(proxy_name)

                            response = {
                                "type": MessageType.CLEANUP_RESPONSE.value,
                                "data": {
                                    "proxy_name": proxy_name,
                                    "status": "success",
                                },
                            }
                            writer.write(msgpack.packb(response))
                            await writer.drain()
                            logger.info(f"Sent cleanup response to client {proxy_name}")

                        elif msg_type == MessageType.CLIENT_HEARTBEAT.value:
                            proxy_name = self.decode_if_bytes(
                                self.get_data_field(data, "proxy_name")
                            )
                            self.client_heartbeats[proxy_name] = (
                                asyncio.get_event_loop().time()
                            )
                            await self.send_heartbeat(writer, proxy_name)
                            continue

                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
                        logger.exception(e)

        except Exception as e:
            logger.error(f"Client connection error: {e}")
        finally:
            writer.close()
            await writer.wait_closed()
            logger.info(f"Client connection closed from {client_addr}")

    async def start(self):
        server = await asyncio.start_server(
            self.handle_client, "0.0.0.0", self.server_port
        )

        _heartbeat_check_task = asyncio.create_task(self.check_client_heartbeats())

        async with server:
            logger.info(f"Server started on port {self.server_port}")
            await server.serve_forever()
