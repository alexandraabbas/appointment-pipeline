"""Publishes messages to Pub/Sub every minute."""
import time
import argparse
from google.cloud import pubsub_v1


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project_id", dest="project_id", default="appointment-streaming-test"
    )
    parser.add_argument("--topic_name", dest="topic_name", default="appointment-topic")
    parser.add_argument(
        "--data_file_name", dest="data_file_name", default="appointment_events.txt"
    )

    return parser.parse_args()


def read_json_file(filename):
    events = []
    for line in open(filename, "r"):
        events.append(line.rstrip())

    return events


def main(args):
    events = read_json_file(args.data_file_name)

    publisher = pubsub_v1.PublisherClient()
    topic_path = "projects/{project_id}/topics/{topic}".format(
        project_id=args.project_id, topic=args.topic_name
    )

    for index in range(0, len(events)):
        publisher.publish(topic_path, data=events[index].encode("utf-8"))
        print("Message number {} published. Waiting for 60 seconds...".format(index))
        time.sleep(60)

        if index == len(events) - 1:
            index = 0


if __name__ == "__main__":
    args = get_args()
    main(args)
