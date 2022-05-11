import socket
import json
from tweepy import StreamingClient, StreamRule


bearer_token = 'AAAAAAAAAAAAAAAAAAAAABDYcQEAAAAAQyv9Elq%2BmiR12ZsEYl5oAH0YcPw%3DjHNjsnwHpgxGD4cum34XG9Xy8Jxyyl21KXlueKoUpsILCibgni'
#

def main():
    s = socket.socket()
    host = '0.0.0.0'
    port = 5555
    s.bind((host, port))
    s.listen(4)
    c_socket, addr = s.accept()
    print(addr)


def send_data(c_socket, filter_word):
    streaming_client = StreamingClient(
        bearer_token=bearer_token,
        return_type=TweetListener(c_socket),
        wait_on_rate_limit=True)
    streaming_client.add_rules(StreamRule(filter_word))
    streaming_client.filter()


class TweetListener(StreamingClient):
    def __init__(self, csocket, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client_socket = csocket

    def on_data(self, data):
        try:
            tweet = json.loads(data)
            # Extended tweets are contained in full_text rather than text
            if 'extended_tweet' in tweet:
                self.client_socket \
                    .send(str(tweet['extended_tweet']['full_text'] + 't_end').encode('utf-8'))
            else:
                self.client_socket \
                    .send(str(tweet['text'] + 't_end').encode('utf-8'))
        except BaseException as e:
            print('Error in on_data: ' + str(e))
        return True

    def on_errors(self, errors):
        print('Error: ' + str(errors))
        return True


if __name__ == '__main__':
    main()
