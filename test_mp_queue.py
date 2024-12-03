from dpkt.pcap import UniversalReader
import os
import multiprocessing as mp
import pandas as pd
import time
from tqdm import tqdm

from nfstream.streamer import NFStreamer


def job(queue, pcap_path):
    with open(pcap_path, "rb") as file_handle:
        reader = UniversalReader(file_handle)
        for ts, packet in tqdm(reader):
            queue.put((ts, packet))
            return


def main():
    # mp.Queue()
    file_path = "/home/nheaven/cicids2017/deduped/Monday-WorkingHours.pcap"
    streamer = NFStreamer(source=file_path, n_meters=1, idle_timeout=5, active_timeout=120, n_dissections=0)
    pcap_csv_output = "pcap_output_standard.csv"
    streamer.to_csv(pcap_csv_output)
    pcap_df = pd.read_csv(pcap_csv_output)
    print(pcap_df)
    return

    queue = mp.Queue()
    proc = mp.Process(target=job, args=(queue, file_path,))
    streamer = NFStreamer(source=queue, n_meters=1, idle_timeout=5, active_timeout=120, n_dissections=0)
    queue_csv_output = "pcap_output_queue.csv"
    proc.start()
    streamer.to_csv(queue_csv_output)
    proc.join()
    queue_df = pd.read_csv(queue_csv_output)
    print(queue_df)


if __name__ == "__main__":
    main()
