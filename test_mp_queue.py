from dpkt.pcap import UniversalReader
import os
import multiprocessing as mp
import pandas as pd
import time
from tqdm import tqdm

from nfstream.streamer import NFStreamer, SupportedDLT


def job(queue, pcap_path):
    with open(pcap_path, "rb") as file_handle:
        reader = UniversalReader(file_handle)
        for ts, packet in tqdm(reader):
            queue.put((ts, packet))
            # return


def main():
    # file_path = "/home/nheaven/cicids2017/deduped/Monday-WorkingHours.pcap"
    file_path = "/home/nheaven/data/bigFlows.pcap"
    
    streamer = NFStreamer(source=file_path, n_meters=0, idle_timeout=5, active_timeout=120, n_dissections=0)
    pcap_csv_output = "pcap_output_standard.csv"
    start_pcap_read = time.time()
    streamer.to_csv(pcap_csv_output)
    end_pcap_read = time.time()
    pcap_df = pd.read_csv(pcap_csv_output)
    print(pcap_df)

    queue = mp.Manager().Queue()
    proc = mp.Process(target=job, args=(queue, file_path,))
    streamer = NFStreamer(
        source=queue, n_meters=0, idle_timeout=5, active_timeout=120, n_dissections=0, datalink_type=SupportedDLT.DLT_EN10MB
    )
    queue_csv_output = "pcap_output_queue.csv"
    proc.start()
    start_queue_read = time.time()
    streamer.to_csv(queue_csv_output)
    end_queue_read = time.time()
    proc.join()
    queue_df = pd.read_csv(queue_csv_output)
    print(queue_df)

    print(end_pcap_read - start_pcap_read)
    print(end_queue_read - start_queue_read)


if __name__ == "__main__":
    main()
