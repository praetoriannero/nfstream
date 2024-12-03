from dpkt.pcap import UniversalReader
import os
import multiprocessing as mp

from nfstream.streamer import NFStreamer


def job(queue, pcap_path):
    with open(pcap_path, "rb") as file_handle:
        reader = UniversalReader(file_handle)
        for ts, packet in reader:
            queue.put((ts, packet))


def main():
    file_path = "/home/heaven/source/cic_ids_2017/deduped/Monday-WorkingHours_deduped.pcap"
    streamer = NFStreamer(source=file_path, n_meters=3, idle_timeout=5, active_timeout=120,)
    streamer.to_csv("pcap_output.csv")


if __name__ == "__main__":
    main()
