import asyncio
import json
import os
from datetime import datetime
import pytz
from websockets import connect
from termcolor import cprint

websocket_url_base = 'wss://fstream.binance.com/ws/!forceOrder@arr'
liquidation_filename = 'binance_liquidations.csv'

if not os.path.isfile(liquidation_filename):
    with open(liquidation_filename, 'w') as f:
        f.write(",".join([
            'symbol', 'side','order_type', 'time_in_force', 'original_quantity',
            'price', 'average_price', 'order_status', 'order_last_filled_quantity',
            'order_filled_accumulated_quantity', 'order_trade_time', 'usd_size']) + '\n')
        
async def binance_liquidation_stream(uri, filename):
    async with connect(uri) as websocket:
        while True:
            try:
                message = await websocket.recv()  
                order_data = json.loads(message)['o']
                symbol = order_data['s'].replace('USDT', '')
                side = order_data['S']
                timestamp = int(order_data['T'])
                filled_quantity = float(order_data['z'])
                price = float(order_data['p'])
                usd_size = filled_quantity * price
                est = pytz.timezone('US/Eastern')
                readable_trade_time = datetime.fromtimestamp(timestamp / 1000, est).strftime('%H:%M:%S')

                if usd_size > 3000:
                    liquidation_type = 'Long Liquidations' if side == 'SELL' else 'Short Liquidations'
                    color = 'green' if side == 'SELL' else 'red'
                    stars = ''

                    attrs = []

                    if usd_size >= 50000:
                        attrs.append("bold")
                        stars = '*'
                    if usd_size >= 100000:
                        stars = '**'

                    if usd_size >= 250000:
                        attrs.append("blink")
                        stars = '***'
                    if usd_size >= 500000:
                        
                        if side == 'SELL':
                            color = 'magenta'
                        else:
                            color = 'blue'

                    output = f"{stars}{liquidation_type} {symbol} {readable_trade_time} ${usd_size:,.0f}{stars}"
                    cprint(output, 'white', f'on_{color}', attrs=attrs)
                    print()

                    payload_values = [str(order_data.get(key)) for key in ['s', 'S', 'o', 'f', 'q', 'p', 'ap', 'X', 'l', 'z', 'T']]
                    payload_values.append(str(usd_size))

                    with open(filename, 'a') as f:
                        trade_info = ','.join(payload_values) + '\n'
                        trade_info = trade_info.replace('USDT', '')
                        f.write(trade_info)
            
            except Exception as e:
                print(f"An error occurred: {e}") 
                await asyncio.sleep(5)

asyncio.run(binance_liquidation_stream(websocket_url_base, liquidation_filename))
