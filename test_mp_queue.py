from dpkt.pcap import UniversalReader
import os
import multiprocessing as mp
import pandas as pd
import time

from nfstream.streamer import NFStreamer


def job(queue, pcap_path):
    with open(pcap_path, "rb") as file_handle:
        reader = UniversalReader(file_handle)
        for ts, packet in reader:
            queue.put((ts, packet))


def main():
    file_path = "/home/nheaven/cicids2017/deduped/Monday-WorkingHours.pcap"
    streamer = NFStreamer(source=file_path, n_meters=3, idle_timeout=5, active_timeout=120, n_dissections=0)
    pcap_csv_output = "pcap_output.csv"
    streamer.to_csv(pcap_csv_output)
    pcap_df = pd.read_csv(pcap_csv_output)
    print(pcap_df)
    


if __name__ == "__main__":
    main()
