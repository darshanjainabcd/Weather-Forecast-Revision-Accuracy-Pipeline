import argparse
from src.producer.main import run

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["stdout", "kinesis"], default="stdout")
    p.add_argument("--forecast-days", type=int, default=3)
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    run(forecast_days=args.forecast_days, mode=args.mode)
