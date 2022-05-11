import tweepy
import socket
import json
from tweepy import StreamingClient


def main():
    streaming_client = StreamingClient(bearer_token='AAAAAAAAAAAAAAAAAAAAABDYcQEAAAAAQyv9Elq%2BmiR12ZsEYl5oAH0YcPw%3DjHNjsnwHpgxGD4cum34XG9Xy8Jxyyl21KXlueKoUpsILCibgni',
                                       wait_on_rate_limit=True)
    streaming_client.add_rules(tweepy.StreamRule("Tweepy"))
    streaming_client.filter()

    print(streaming_client.get_rules())


class ConnectionTester(tweepy.StreamingClient):

    def on_connection_error(self):
        self.disconnect()


if __name__ == '__main__':
    main()
