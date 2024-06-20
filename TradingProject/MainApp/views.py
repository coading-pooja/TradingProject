from django.shortcuts import render
import csv
import json
from datetime import datetime, timedelta
from django.shortcuts import render
from django.http import HttpResponse
from .forms import UploadFileForm
from .models import Candle
import aiofiles
import asyncio

def index(request):
    if request.method == 'POST':
        form = UploadFileForm(request.POST, request.FILES)
        if form.is_valid():
            file = request.FILES['file']
            timeframe = form.cleaned_data['timeframe']
            handle_uploaded_file(file, timeframe)
            return HttpResponse("File uploaded and processed successfully.")
    else:
        form = UploadFileForm()
    return render(request, 'MainApp/index.html', {'form': form})

def handle_uploaded_file(file, timeframe):
    # Process the file (saving, reading, converting)
    file_path = f'media/{file.name}'
    with open(file_path, 'wb+') as destination:
        for chunk in file.chunks():
            destination.write(chunk)
    asyncio.run(process_file(file_path, timeframe))

async def process_file(file_path, timeframe):
    candles = []
    async with aiofiles.open(file_path, mode='r') as f:
        reader = csv.DictReader(await f.readlines())
        for row in reader:
            date_time = datetime.strptime(f"{row['DATE']} {row['TIME']}", '%Y%m%d %H:%M')
            candles.append(Candle(
                open=float(row['OPEN']),
                high=float(row['HIGH']),
                low=float(row['LOW']),
                close=float(row['CLOSE']),
                date=date_time
            ))

    converted_candles = convert_timeframe(candles, timeframe)
    output_file_path = file_path.replace('.csv', '.json')
    with open(output_file_path, 'w') as f:
        json.dump([c.__dict__ for c in converted_candles], f)

def convert_timeframe(candles, timeframe):
    timeframe_delta = timedelta(minutes=timeframe)
    converted = []
    current_candle = candles[0]
    high = current_candle.high
    low = current_candle.low

    for candle in candles[1:]:
        if candle.date - current_candle.date < timeframe_delta:
            high = max(high, candle.high)
            low = min(low, candle.low)
        else:
            current_candle.high = high
            current_candle.low = low
            converted.append(current_candle)
            current_candle = candle
            high = candle.high
            low = candle.low

    current_candle.high = high
    current_candle.low = low
    converted.append(current_candle)
    return converted
