import asyncio

import json
import websockets

async def explore():
    url = "wss://advanced-trade-ws.coinbase.com/ws"
    async with websockets.connect(url) as ws:
        subscribe = {
            "type": "subscribe",
            "product_ids": ["BTC-USD"],                                                                                                                                 
            "channel": "market_trades"                                                                                                                                            
            }                                                                                                                                                               
        await ws.send(json.dumps(subscribe))                                                                                                                            

        for _ in range(5):           # grab 5 messages
            msg = await ws.recv()
            parsed = json.loads(msg)                                                                                                                                    
            print(json.dumps(parsed, indent=2))  # pretty print
asyncio.run(explore())