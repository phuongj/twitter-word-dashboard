import socket
import json
from tweepy import StreamingClient, StreamRule

bearer_token = 'AAAAAAAAAAAAAAAAAAAAAOQxcgEAAAAAMbkrXWAjBEnYH1nbQtBLzVcohHU%3DAYD2w0B1VPi1CR1W6D6grpMDCuA0MUeyhHZP5ZPUjKPCM1y6XS'


def main():
    s = socket.socket()
    host = '0.0.0.0'
    port = 9999
    s.bind((host, port))
    print('Socket is ready')
    s.listen(5)
    print('Socket is listening.')
    c_socket, addr = s.accept()
    print("Connection established.")
    send_data(c_socket, filter_word=['fish'])


def send_data(c_socket, filter_word):
    streaming_client = TweetListener(
        bearer_token=bearer_token,
        csocket=c_socket)
    #streaming_client.add_rules(StreamRule(filter_word))
    streaming_client.add_rules(StreamRule("fish lang:en"))
    #streaming_client.delete_rules('1525077818727464961')
    streaming_client.filter()

    #streaming_client.sample()


def process_tweet(tweet):
    tweet = tweet.replace('RT ', '')
    return tweet


class TweetListener(StreamingClient):
    def __init__(self, bearer_token, csocket, **kwargs):
        super().__init__(bearer_token, **kwargs)
        self.bearer_token = bearer_token
        self.client_socket = csocket
        self.wait_on_rate_limit = True

    def on_data(self, data):
        try:
            print('New tweet:')
            tweet = json.loads(data)
            # Extended tweets are contained in full_text rather than text
            if 'extended_tweet' in tweet['data']:
                self.client_socket \
                    .send(
                        process_tweet(
                            str(tweet['data']['extended_tweet']['full_text'] + 't_end')
                        ).encode('utf-8')
                    )
                print(str(tweet['data']['extended_tweet']['full_text']))
            else:
                self.client_socket \
                    .send(
                        process_tweet(
                            str(tweet['data']['text'] + 't_end')
                        ).encode('utf-8')
                    )
                print(str(tweet['data']['text'] + 't_end'))
        except BaseException as e:
            print('Error in on_data: ' + str(e))
        return True

    def on_errors(self, errors):
        print('Error: ' + str(errors))
        return True


if __name__ == '__main__':
    main()
