import pika, sys, os, time
from send import email


def main():

    # rabbitmq connection service name is rabbitmq
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
    channel = connection.channel()

    def callback(ch, method, properties, body):
        err = email.notification(body)
        if err:
            # delivery tag is to identify message which had error
            # message not removed from queue as it can be processed again by another server
            ch.basic_nack(delivery_tag=method.delivery_tag)
        else:
            # No issue with conversion
            ch.basic_ack(delivery_tag=method.delivery_tag)


    # to consume message
    channel.basic_consume(
        queue=os.environ.get("MP3_QUEUE"), on_message_callback=callback
    )

    print("Waiting for messages. To exit press CTRL+C")

    # run consumer
    channel.start_consuming()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)