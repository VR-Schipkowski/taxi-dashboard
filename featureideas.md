## Overall Pipeline
- implment performance/latency metric -> timestamp when a datapoint is fed into the pipline and when it is send to dashboard

## Kafka
- Kafka as backbone
- send data processed in flink back to kafka
- redis listens to kafka and sends to frontend
- database listens to kafka
- Try kafka watermarks for event timestamps to ensure correct streaming to the frontend

## Frontend
- Clock
- night day time shift
- Speedfacktor of the pipline
- Speedwarning

## Presentation do's and don'ts
- Leave out agenda
- Make the architecture more easily understandable
- as little text as possible
- as much demonstration as possible
- Treat it more like a pitch -> storytelling