import json
# import uuid
import asyncio
import threading
from time import sleep

import confluent_kafka
import websockets
from confluent_kafka.admin import AdminClient

normalized_trade_topic = 'trades.normalized'
raw_trade_topic = 'trades.raw'
symbols = ['btcusdt', 'ethusdt']

conf = {
    'bootstrap.servers': 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
}


def check_kafka_connection() -> bool:
    '''Check if the Kafka brokers are reachable'''

    try:
        client = AdminClient(conf)  # type: ignore

        metadata = client.list_topics(timeout=5)
        print('Connection successful!')
        print(f'Cluster ID: {metadata.cluster_id}')
        print(f'Active brokers: {len(metadata.brokers)}')

        return True
    except Exception as e:
        print(f'Failed to connect to Kafka: {e}')
        return False


def topic_exists(topic_name: str) -> bool:
    '''Check if expected topics already exist'''

    admin_client = AdminClient(conf)  # type: ignore
    metadata = admin_client.list_topics(timeout=10)

    return topic_name in metadata.topics


def delivery_report(err, msg) -> None:
    '''Callback triggered by Kafka producer
    Args:
        err (KafkaError): The error object if the message failed to deliver
        msg (Message): The object if the message is delivered successfully
    '''

    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(
            f'Message delivered to {msg.topic()} '
            f'[{msg.partition()}] offset {msg.offset()}'
        )


def normalize_trade(data) -> dict:
    '''Normalizes a raw trade message from the Binance WebSocket API
    Args:
        data (dict): The raw JSON message received from Binance stream
    Returns:
        dict: A dictionary containing normalized trade data
    '''

    return {
        'event': data['e'],
        'symbol': data['s'],
        'trade_id': data['t'],
        'price': float(data['p']),
        'qty': float(data['q']),
        'trade_time': data['T'],
        'is_maker': data['m']
    }


async def subscribe(symbol: str) -> None:
    uri = f'wss://stream.binance.com:443/ws/{symbol}@trade'

    while True:
        try:
            while True:
                if check_kafka_connection():
                    break

                print('Trying connection...')
                await asyncio.sleep(2)

            producer = confluent_kafka.Producer(conf)

            async with websockets.connect(uri) as websocket:
                '''
                subscribe só é usado quando conectar no websocket
                sem informar o endpoint.
                ex: websockets.connect('wss://stream.binance.com:443/ws')
                '''
                # subscribe_message = {
                #     'id': str(uuid.uuid4()),
                #     'method': 'SUBSCRIBE',
                #     'params': [f'{symbol}@trade']
                # }

                # await websocket.send(json.dumps(subscribe_message))

                async for message in websocket:
                    try:
                        print(f'Received: {message}')

                        producer.produce(
                            normalized_trade_topic,
                            str(
                                json.dumps(
                                    normalize_trade(json.loads(message))
                                )
                            ),
                            callback=delivery_report
                        )
                        producer.flush()

                        producer.produce(
                            raw_trade_topic,
                            str(json.loads(message)),
                            callback=delivery_report
                        )
                        producer.flush()

                    except Exception as e:
                        print(
                            f'Failed do process message or produce to Kafka: '
                            f'{e}'
                        )
        except websockets.exceptions.ConnectionClosed:
            print('Binance websocket connection failed.')
            print('Trying again...')
            await asyncio.sleep(2)
        except Exception as e:
            print(f'Unknown error: {e}')
            print('Trying again')
            await asyncio.sleep(2)


def run_async(symbol: str) -> None:
    asyncio.run(subscribe(symbol))


if __name__ == '__main__':

    while True:
        if check_kafka_connection():
            break

        print('Trying connection...')
        sleep(2)

    if not topic_exists(normalized_trade_topic):
        print(f'Tópico {normalized_trade_topic} não existe no kafka!')
        exit()

    if not topic_exists(raw_trade_topic):
        print(f'Tópico {raw_trade_topic} não existe no kafka!')
        exit()

    threads: list = []

    for symbol in symbols:
        t = threading.Thread(target=run_async, args=(symbol,))
        threads.append(t)

    for t in threads:
        t.start()

    for t in threads:
        t.join()
